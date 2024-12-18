﻿using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using ImcFamosFile;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

[ExtensionDescription(
    "Provides access to databases with Famos files.",
    "https://github.com/Apollo3zehn/nexus-sources-famos",
    "https://github.com/Apollo3zehn/nexus-sources-famos")]
public class Famos : StructuredFileDataSource
{
    record CatalogDescription(
        string Title,
        Dictionary<string, IReadOnlyList<FileSource>> FileSourceGroups,
        JsonElement? AdditionalProperties);

    #region Fields

    private Dictionary<string, CatalogDescription> _config = default!;

    #endregion

    #region Methods

    protected override async Task InitializeAsync(CancellationToken cancellationToken)
    {
        var configFilePath = Path.Combine(Root, "config.json");

        if (!File.Exists(configFilePath))
            throw new Exception($"Configuration file {configFilePath} not found.");

        var jsonString = await File.ReadAllTextAsync(configFilePath, cancellationToken);
        _config = JsonSerializer.Deserialize<Dictionary<string, CatalogDescription>>(jsonString) ?? throw new Exception("config is null");
    }

    protected override Task<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>> GetFileSourceProviderAsync(
        CancellationToken cancellationToken)
    {
        return Task.FromResult<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>>(
            catalogId => _config[catalogId].FileSourceGroups);
    }

    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
    {
        if (path == "/")
            return Task.FromResult(_config.Select(entry => new CatalogRegistration(entry.Key, entry.Value.Title)).ToArray());

        else
            return Task.FromResult(Array.Empty<CatalogRegistration>());
    }

    protected override Task<ResourceCatalog> EnrichCatalogAsync(ResourceCatalog catalog, CancellationToken cancellationToken)
    {
        var catalogDescription = _config[catalog.Id];

        foreach (var (fileSourceId, fileSourceGroup) in catalogDescription.FileSourceGroups)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                var filePaths = default(string[]);
                var catalogSourceFiles = fileSource.AdditionalProperties?.GetStringArray("CatalogSourceFiles");

                if (catalogSourceFiles is not null)
                {
                    filePaths = catalogSourceFiles
                        .Where(filePath => filePath is not null)
                        .Select(filePath => Path.Combine(Root, filePath!))
                        .ToArray();
                }
                else
                {
                    if (!TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = [filePath];
                }

                cancellationToken.ThrowIfCancellationRequested();

                foreach (var filePath in filePaths)
                {
                    using var famosFile = FamosFile.Open(filePath);
                    var resources = GetResources(famosFile, fileSourceId);

                    var newCatalog = new ResourceCatalogBuilder(id: catalog.Id)
                        .AddResources(resources)
                        .Build();

                    catalog = catalog.Merge(newCatalog);
                }
            }
        }

        return Task.FromResult(catalog);
    }

    protected override Task ReadAsync(ReadInfo info, ReadRequest[] readRequests, CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            foreach (var readRequest in readRequests)
            {
                using var famosFile = FamosFile.Open(info.FilePath);

                var channels = famosFile.Groups.SelectMany(group => group.Channels).Concat(famosFile.Channels).ToList();
                var famosFileChannel = channels.FirstOrDefault(current => current.Name == readRequest.OriginalResourceName);

                if (famosFileChannel != default)
                {
                    var component = famosFile.FindComponent(famosFileChannel);
                    var fileDataType = FamosUtilities.GetNexusDataTypeFromFamosDataType(component.PackInfo.DataType);

                    if (fileDataType == 0)
                        throw new Exception($"The data type '{component.PackInfo.DataType}' is not supported.");

                    // invoke generic 'ReadData' method
                    var methodName = nameof(ReadData);
                    var flags = BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;
                    var genericType = FamosUtilities.GetTypeFromNexusDataType(fileDataType);
                    var parameters = new object[] { famosFile, famosFileChannel };
                    var result = (double[])FamosUtilities.InvokeGenericMethod(this, methodName, flags, genericType, parameters);
                    var elementSize = readRequest.CatalogItem.Representation.ElementSize;

                    cancellationToken.ThrowIfCancellationRequested();

                    // write data
                    if (result.Length == info.FileLength)
                    {
                        var byteResult = MemoryMarshal.AsBytes(result.AsSpan());
                        var offset = (int)info.FileOffset * elementSize;
                        var length = (int)info.FileBlock * elementSize;

                        byteResult
                            .Slice(offset, length)
                            .CopyTo(readRequest.Data.Span);

                        readRequest
                            .Status
                            .Span
                            .Fill(1);
                    }
                    // skip data
                    else
                    {
                        Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                    }
                }
            }
        }, cancellationToken);
    }

    private static double[] ReadData<T>(FamosFile famosFile, FamosFileChannel channel) where T : unmanaged
    {
        var applyCalibration = true;

        if (!applyCalibration)
            throw new Exception("Currently, only applyCalibration = true is supported.");

        var channelData = famosFile.ReadSingle(channel);
        var componentsData = channelData.ComponentsData.First();
        var rawData = ((FamosFileComponentData<T>)componentsData).Data;
        var rawDataType = channelData.ComponentsData.First().PackInfo.DataType;

        // return data
        var analogComponent = famosFile.FindComponent(channel) as FamosFileAnalogComponent;
        var calibrationInfo = analogComponent?.CalibrationInfo;
        var doubleData = FamosUtilities.ToDouble(rawData);

        if (calibrationInfo != default && calibrationInfo.ApplyTransformation)
        {
            Parallel.For(0, doubleData.Length, i =>
            {
                doubleData[i] = (double)((decimal)doubleData[i] * calibrationInfo.Factor + calibrationInfo.Offset);
            });
        }

        return doubleData;
    }

    private static List<Resource> GetResources(FamosFile famosFile, string fileSourceId)
    {
        var fields = famosFile.Fields.Where(field => field.Type == FamosFileFieldType.MultipleYToSingleEquidistantTime).ToList();

        return fields.SelectMany(field =>
        {
            var resources = new List<Resource>();

            foreach (var component in field.Components)
            {
                var analogComponent = component as FamosFileAnalogComponent;

                if (analogComponent == default)
                    continue;

                var famosFileChannel = component.Channels.First();

                // resource id
                if (!TryEnforceNamingConvention(famosFileChannel.Name, out var resourceId))
                    continue;

                // samples per day
                var xAxisScaling = component.XAxisScaling;

                if (xAxisScaling?.Unit != "s")
                    throw new Exception("Could not determine the sample rate.");

                var samplePeriod = TimeSpan.FromSeconds((double)xAxisScaling.DeltaX);

                // group name
                //var group = famosFile.Groups.FirstOrDefault(group => group.Resources.Contains(famosFileChannel));
                //var groupName = group != default ? group.Name : "General";

                // data type
                //var dataType = GetNexusDataTypeFromFamosFileDataType(component.PackInfo.DataType);
                var dataType = NexusDataType.FLOAT64;

                // unit
                var unit = analogComponent.CalibrationInfo == default ? string.Empty : analogComponent.CalibrationInfo.Unit;

                // representation
                var representation = new Representation(
                    dataType: dataType,
                    samplePeriod: samplePeriod);

                // create resource
                var resource = new ResourceBuilder(id: resourceId)
                    .WithUnit(unit)
                    .WithGroups(fileSourceId)
                    .WithFileSourceId(fileSourceId)
                    .WithOriginalName(famosFileChannel.Name)
                    .AddRepresentation(representation)
                    .Build();

                resources.Add(resource);
            }

            return resources;
        }).ToList();
    }

    private static bool TryEnforceNamingConvention(string resourceId, [NotNullWhen(returnValue: true)] out string newResourceId)
    {
        newResourceId = resourceId;
        newResourceId = Resource.InvalidIdCharsExpression.Replace(newResourceId, "");
        newResourceId = Resource.InvalidIdStartCharsExpression.Replace(newResourceId, "");

        return Resource.ValidIdExpression.IsMatch(newResourceId);
    }

    #endregion
}
