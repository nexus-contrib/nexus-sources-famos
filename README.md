# Nexus.Sources.Famos

This data source extension makes it possible to read data files in the Famos format into Nexus.

To use it, put a `config.json` with the following sample content into the database root folder:

```json
{
  "/A/B/C": {
    "FileSourceGroups": [
      {
        "Name": "raw",
        "PathSegments": [
          "'DATA'",
          "yyyy-MM"
        ],
        "FileTemplate": "yyyy-MM-dd_HH-mm-ss'.dat'",
        "FilePeriod": "00:10:00",
        "UtcOffset": "00:00:00"
      }
    ]
  }
}
```

Please see the [tests](tests/Nexus.Sources.Famos.Tests) folder for a complete sample.