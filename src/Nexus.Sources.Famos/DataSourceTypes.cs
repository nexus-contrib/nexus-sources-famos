using Nexus.Extensibility;
using System;
using System.Collections.Generic;

namespace Nexus.Sources
{
    internal record CatalogDescription(
        string Title,
        ExtendedFileSource[] FileSources,
        Dictionary<string, string>? CustomParameters = default);

    internal record ExtendedFileSource : FileSource
    {
        public ExtendedFileSource(
            string[] PathSegments,
            string FileTemplate,
            string? FileDateTimePreselector,
            string? FileDateTimeSelector,
            TimeSpan FilePeriod,
            TimeSpan UtcOffset,
            string Name,
            string[] CatalogSourceFiles,
            Dictionary<string, string>? CustomParameters
        )
            : base(PathSegments, FileTemplate, FileDateTimePreselector, FileDateTimeSelector, FilePeriod, UtcOffset)
        {
            this.Name = Name;
            this.CatalogSourceFiles = CatalogSourceFiles;
            this.CustomParameters = CustomParameters;
        }

        public string Name { get; init; }

        public string[] CatalogSourceFiles { get; init; }

        public Dictionary<string, string>? CustomParameters { get; init; }
    }
}
