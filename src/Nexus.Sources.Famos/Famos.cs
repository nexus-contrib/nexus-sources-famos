using ImcFamosFile;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Nexus.Sources
{
    [ExtensionDescription("Provides access to databases with Famos files.")]
    public class Famos : StructuredFileDataSource
    {
        #region Fields

        private Dictionary<string, CatalogDescription> _config = null!;

        #endregion

        #region Properties

        private DataSourceContext Context { get; set; } = null!;

        #endregion

        #region Methods

        protected override async Task SetContextAsync(DataSourceContext context, CancellationToken cancellationToken)
        {
            this.Context = context;

            var configFilePath = Path.Combine(this.Root, "config.json");

            if (!File.Exists(configFilePath))
                throw new Exception($"Configuration file {configFilePath} not found.");

            var jsonString = await File.ReadAllTextAsync(configFilePath, cancellationToken);
            _config = JsonSerializer.Deserialize<Dictionary<string, CatalogDescription>>(jsonString) ?? throw new Exception("config is null");
        }

        protected override Task<FileSourceProvider> GetFileSourceProviderAsync(CancellationToken cancellationToken)
        {
            var allFileSources = _config.ToDictionary(
                config => config.Key,
                config => config.Value.FileSources.Cast<FileSource>().ToArray());

            var fileSourceProvider = new FileSourceProvider(
                All: allFileSources,
                Single: catalogItem =>
                {
                    var properties = catalogItem.Resource.Properties;

                    if (properties is null)
                        throw new ArgumentNullException(nameof(properties));

                    return allFileSources[catalogItem.Catalog.Id]
                        .First(fileSource => ((ExtendedFileSource)fileSource).Name == properties["FileSource"]);
                });

            return Task.FromResult(fileSourceProvider);
        }

        protected override Task<string[]> GetCatalogIdsAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(_config.Keys.ToArray());
        }

        protected override Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
        {
            var catalogDescription = _config[catalogId];
            var catalog = new ResourceCatalog(id: catalogId);

            foreach (var fileSource in catalogDescription.FileSources)
            {
                var filePaths = default(string[]);

                if (fileSource.CatalogSourceFiles is not null)
                {
                    filePaths = fileSource.CatalogSourceFiles
                        .Select(filePath => Path.Combine(this.Root, filePath))
                        .ToArray();
                }
                else
                {
                    if (!this.TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = new[] { filePath };
                }

                cancellationToken.ThrowIfCancellationRequested();

                foreach (var filePath in filePaths)
                {
                    using var famosFile = FamosFile.Open(filePath);
                    var resources = this.GetResources(famosFile, fileSource);

                    var newCatalog = new ResourceCatalogBuilder(id: catalogId)
                        .AddResources(resources)
                        .Build();

                    catalog = catalog.Merge(newCatalog, MergeMode.NewWins);
                }
            }

            return Task.FromResult(catalog);
        }

        protected override Task ReadSingleAsync(ReadInfo info, CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                using var famosFile = FamosFile.Open(info.FilePath);

                var channels = famosFile.Groups.SelectMany(group => group.Channels).Concat(famosFile.Channels).ToList();

                var famosFileResource = channels.FirstOrDefault(current =>
                    FamosUtilities.EnforceNamingConvention(current.Name) == info.CatalogItem.Resource.Id);

                if (famosFileResource != null)
                {
                    var component = famosFile.FindComponent(famosFileResource);
                    var fileDataType = FamosUtilities.GetNexusDataTypeFromFamosDataType(component.PackInfo.DataType);

                    if (fileDataType == 0)
                        throw new Exception($"The data type '{component.PackInfo.DataType}' is not supported.");

                    // invoke generic 'ReadData' method
                    var methodName = nameof(Famos.ReadData);
                    var flags = BindingFlags.NonPublic | BindingFlags.Instance;
                    var genericType = FamosUtilities.GetTypeFromNexusDataType(fileDataType);
                    var parameters = new object[] { famosFile, famosFileResource };
                    var result = (double[])FamosUtilities.InvokeGenericMethod(this, methodName, flags, genericType, parameters);

                    cancellationToken.ThrowIfCancellationRequested();

                    // write data
                    if (result.Length == info.FileLength)
                    {
                        var byteResult = MemoryMarshal.AsBytes(result.AsSpan());
                        var offset = (int)info.FileOffset * info.CatalogItem.Representation.ElementSize;

                        byteResult
                            .Slice(offset)
                            .CopyTo(info.Data.Span);

                        info
                            .Status
                            .Span
                            .Fill(1);
                    }
                    // skip data
                    else
                    {
                        this.Context.Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                    }
                }
            });
        }

        private double[] ReadData<T>(FamosFile famosFile, FamosFileChannel channel) where T : unmanaged
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

            if (calibrationInfo != null && calibrationInfo.ApplyTransformation)
            {
                Parallel.For(0, doubleData.Length, i =>
                {
                    doubleData[i] = (double)((decimal)doubleData[i] * calibrationInfo.Factor + calibrationInfo.Offset);
                });
            }

            return doubleData;
        }

        private List<Resource> GetResources(FamosFile famosFile, ExtendedFileSource fileSource)
        {
            var fields = famosFile.Fields.Where(field => field.Type == FamosFileFieldType.MultipleYToSingleEquidistantTime).ToList();

            return fields.SelectMany(field =>
            {
                var resources = new List<Resource>();

                foreach (var component in field.Components)
                {
                    var analogComponent = component as FamosFileAnalogComponent;

                    if (analogComponent == null)
                        continue;

                    var famosFileResource = component.Channels.First();

                    // resource id
                    var resourceId = FamosUtilities.EnforceNamingConvention(famosFileResource.Name);

                    // samples per day
                    var xAxisScaling = component.XAxisScaling;

                    if (xAxisScaling?.Unit != "s")
                        throw new Exception("Could not determine the sample rate.");

                    var samplePeriod = TimeSpan.FromSeconds((double)xAxisScaling.DeltaX);

                    // group name
                    //var group = famosFile.Groups.FirstOrDefault(group => group.Resources.Contains(famosFileResource));
                    //var groupName = group != null ? group.Name : "General";

                    // data type
                    //var dataType = this.GetNexusDataTypeFromFamosFileDataType(component.PackInfo.DataType);
                    var dataType = NexusDataType.FLOAT64;

                    // unit
                    var unit = analogComponent.CalibrationInfo == null ? string.Empty : analogComponent.CalibrationInfo.Unit;

                    // representation
                    var representation = new Representation(
                        dataType: dataType,
                        samplePeriod: samplePeriod);

                    // create resource
                    var resource = new ResourceBuilder(id: resourceId)
                        .WithUnit(unit)
                        .WithGroups(fileSource.Name)
                        .WithProperty("FileSource", fileSource.Name)
                        .AddRepresentation(representation)
                        .Build();

                    resources.Add(resource);
                }

                return resources;
            }).ToList();
        }

        #endregion
    }
}
