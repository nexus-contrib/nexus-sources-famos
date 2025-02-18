using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.InteropServices;
using ImcFamosFile;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

/// <summary>
/// Additional extension-specific settings.
/// </summary>
/// <param name="TitleMap">The catalog ID to title map. Add an entry here to specify a custom catalog title.</param>
public record FamosSettings(
    Dictionary<string, string> TitleMap
);

/// <summary>
/// Additional file source settings.
/// </summary>
/// <param name="CatalogSourceFiles">The source files to populate the catalog with resources.</param>
public record FamosAdditionalFileSourceSettings(
    string[]? CatalogSourceFiles
);

[ExtensionDescription(
    "Provides access to databases with Famos files.",
    "https://github.com/Apollo3zehn/nexus-sources-famos",
    "https://github.com/Apollo3zehn/nexus-sources-famos")]
public class Famos : StructuredFileDataSource<FamosSettings, FamosAdditionalFileSourceSettings>
{
    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(
        string path,
        CancellationToken cancellationToken
    )
    {
        if (path == "/")
        {
            return Task.FromResult(Context.SourceConfiguration.FileSourceGroupsMap
                .Select(entry =>
                    {
                        Context.SourceConfiguration.AdditionalSettings.TitleMap.TryGetValue(entry.Key, out var title);
                        return new CatalogRegistration(entry.Key, title);
                    }
                ).ToArray());
        }

        else
        {
            return Task.FromResult(Array.Empty<CatalogRegistration>());
        }
    }

    protected override Task<ResourceCatalog> EnrichCatalogAsync(ResourceCatalog catalog, CancellationToken cancellationToken)
    {
        var fileSourceGroupsMap = Context.SourceConfiguration.FileSourceGroupsMap[catalog.Id];

        foreach (var (fileSourceId, fileSourceGroup) in fileSourceGroupsMap)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                var additionalSettings = fileSource.AdditionalSettings;
                var filePaths = default(string[]);

                if (additionalSettings.CatalogSourceFiles is not null)
                {
                    filePaths = additionalSettings.CatalogSourceFiles
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

    protected override Task ReadAsync(
        ReadInfo<FamosAdditionalFileSourceSettings> info,
        ReadRequest[] readRequests,
        CancellationToken cancellationToken
        )
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

    private static double[] ReadData<T>(
        FamosFile famosFile,
        FamosFileChannel channel
    ) where T : unmanaged
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

    private static List<Resource> GetResources(
        FamosFile famosFile,
        string fileSourceId
    )
    {
        var fields = famosFile.Fields
            .Where(field => field.Type == FamosFileFieldType.MultipleYToSingleEquidistantTime).ToList();

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
                    throw new Exception("Could not determine the sample period.");

                /* Famos file may contain numbers like 1.9999999999999998E-05
                 * so we round to 100 ns before further processing
                 */
                var samplePeriod = TimeSpan.FromSeconds(Math.Round((double)xAxisScaling.DeltaX, 8));

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
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
