using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Nexus.Sources.Tests
{
    public class FamosTests
    {
        [Fact]
        public async Task ProvidesCatalog()
        {
            // arrange
            var dataSource = new Famos() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                SystemConfiguration: default!,
                SourceConfiguration: default!,
                RequestConfiguration: default!);

            await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

            // act
            var actual = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
            var actualIds = actual.Resources.Select(resource => resource.Id).ToList();
            var actualUnits = actual.Resources.Select(resource => GetPropertyOrDefault(resource.Properties, "unit")).ToList();
            var actualGroups = actual.Resources.SelectMany(resource => GetArrayOrDefault(resource.Properties, "groups")).ToList();
            var actualTimeRange = await dataSource.GetTimeRangeAsync("/A/B/C", CancellationToken.None);

            // assert
            var expectedIds = new List<string>() { "STTZ", "Accx" };
            var expectedUnits = new List<string>() { "mV/V", " V" };
            var expectedGroups = new List<string>() { "raw", "raw" };
            var expectedStartDate = new DateTime(2020, 05, 31, 23, 40, 00);
            var expectedEndDate = new DateTime(2020, 06, 01, 00, 10, 00);

            Assert.True(expectedIds.SequenceEqual(actualIds.Skip(49).Take(2)));
            Assert.True(expectedUnits.SequenceEqual(actualUnits.Skip(49).Take(2)));
            Assert.True(expectedGroups.SequenceEqual(actualGroups.Skip(49).Take(2)));
            Assert.Equal(expectedStartDate, actualTimeRange.Begin);
            Assert.Equal(expectedEndDate, actualTimeRange.End);

            string? GetPropertyOrDefault(JsonElement? element, string propertyName)
            {
                if (!element.HasValue)
                    return default;

                if (element.Value.TryGetProperty(propertyName, out var result))
                    return result.GetString();

                else
                    return default;
            }

            string[] GetArrayOrDefault(JsonElement? element, string propertyName)
            {
                if (!element.HasValue)
                    return new string[0];

                if (element.Value.TryGetProperty(propertyName, out var result))
                    return result.EnumerateArray().Select(current => current.GetString()!).ToArray();

                else
                    return new string[0];
            }
        }

        [Fact]
        public async Task ProvidesDataAvailability()
        {
            // arrange
            var dataSource = new Famos() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                SystemConfiguration: default!,
                SourceConfiguration: default!,
                RequestConfiguration: default!);

            await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

            // act
            var actual = new Dictionary<DateTime, double>();
            var begin = new DateTime(2020, 05, 31, 0, 0, 0, DateTimeKind.Utc);
            var end = new DateTime(2020, 06, 02, 0, 0, 0, DateTimeKind.Utc);

            var currentBegin = begin;

            while (currentBegin < end)
            {
                actual[currentBegin] = await dataSource.GetAvailabilityAsync("/A/B/C", currentBegin, currentBegin.AddDays(1), CancellationToken.None);
                currentBegin += TimeSpan.FromDays(1);
            }

            // assert
            var expected = new SortedDictionary<DateTime, double>(new Dictionary<DateTime, double>
            {
                [new DateTime(2020, 05, 31)] = 2 / 144.0,
                [new DateTime(2020, 06, 01)] = 1 / 144.0
            });

            Assert.True(expected.SequenceEqual(new SortedDictionary<DateTime, double>(actual)));
        }

        [Fact]
        public async Task CanReadFullDay()
        {
            // arrange
            var dataSource = new Famos() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                SystemConfiguration: default!,
                SourceConfiguration: default!,
                RequestConfiguration: default!);

            await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

            // act
            var catalog = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
            var resource = catalog.Resources.First();
            var representation = resource.Representations.First();
            var catalogItem = new CatalogItem(catalog, resource, representation);

            var begin = new DateTime(2020, 05, 31, 0, 0, 0, DateTimeKind.Utc);
            var end = new DateTime(2020, 06, 01, 0, 0, 0, DateTimeKind.Utc);
            var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

            var result = new ReadRequest(catalogItem, data, status);
            await dataSource.ReadAsync(begin, end, new ReadRequest[] { result }, default!, new Progress<double>(), CancellationToken.None);

            // assert
            void DoAssert()
            {
                var data = MemoryMarshal.Cast<byte, double>(result.Data.Span);

                Assert.Equal(0, data[0]);
                Assert.Equal(0, data[2000000]);
                Assert.Equal(0, data[4259999]);
                Assert.Equal(190, data[4260000]);
                Assert.Equal(201, data[4289999]);
                Assert.Equal(201, data[4290000]);
                Assert.Equal(198, data[4319999]);

                Assert.Equal(0, result.Status.Span[0]);
                Assert.Equal(0, result.Status.Span[2000000]);
                Assert.Equal(0, result.Status.Span[4259999]);
                Assert.Equal(1, result.Status.Span[4260000]);
                Assert.Equal(1, result.Status.Span[4289999]);
                Assert.Equal(1, result.Status.Span[4290000]);
                Assert.Equal(1, result.Status.Span[4319999]);
            }

            DoAssert();
        }
    }
}