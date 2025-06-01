# Spark NetCDF Connector

This project implements a Spark connector for reading NetCDF files into Spark DataFrames using Apache Spark and Scala.
This connector uses [NetCDF Java](https://www.unidata.ucar.edu/software/netcdf-java/) to read data from netcdf files.

## Features

- **Custom Schema Support**: Define the schema for NetCDF variables.
- **Partition Handling**: Automatically manages partitions for large datasets.
- **Scalable Performance**: Optimized for distributed computing with Spark.
- **Storage Compatibility**: This connector supports reading NetCDF files from:
    - Local file systems (tested).
    - Amazon S3, see [Dataset URLs](https://docs.unidata.ucar.edu/netcdf-java/5.6/userguide/dataset_urls.html) for configuration (tested).
---

## Requirements

- **Java**: Version 11
- **Apache Spark**: Version 3.x
- **Scala**: Version 2.12,2.13
- **Dependency Management**: SBT, Maven, or similar
- **Unidata repository**: Add Unidata repository, see [Using netCDF-Java Maven Artifacts](https://docs.unidata.ucar.edu/netcdf-java/current/userguide/using_netcdf_java_artifacts.html)

---

## Getting Started

### Add Dependency to Your Project

Update your `build.sbt` file with the following configuration:
```
libraryDependencies += "io.github.rejeb" %% "spark-netcdf" % "VERSION"
``` 

Replace `VERSION` with the specific version of the connector.

---

## Define Your NetCDF Schema

NetCDF requires an explicitly defined schema for variable mapping. Here is an example schema definition:
```scala
val schema = StructType(Seq(
StructField("temperature", FloatType),
StructField("humidity", FloatType),
StructField("timestamp", StringType),
StructField("metadata", ArrayType(StringType))
))
``` 

---

## Load NetCDF Files

Here is how to load a NetCDF file into a DataFrame:

```scala
val spark = SparkSession.builder().appName("NetCDF File Reader").master("local[*]").getOrCreate()
val df = spark.read.format("netcdf")
  .schema(schema)
  .option("path", "/path/to/your/netcdf-file.nc")
  .load()
df.show()
``` 

---

## Configuration Options


| Option              | Description                                           | Required | Default       |
|---------------------|-------------------------------------------------------|----------|---------------|
| `path`              | Path to the NetCDF file                               | Yes      | None          |
| `partitionSize`     | Rows per partition to optimize parallelism            | No       | 20,000 rows   |
| `ignoredDimensions` | Comma-separated list of dimensions to ignore          | No       | None          |

Example with options:

```scala
val df = spark.read .format("netcdf")
  .schema(schema)
  .option("path", "/path/to/file.nc")
  .option("partitionSize", 50000)
  .option("ignoredDimensions", "latitude,longitude")
  .load()
``` 

---

## Supported Data Types

The following **Spark SQL data types** are supported by the NetCDF connector:

- `FloatType`
- `StringType`
- `IntegerType`
- `ArrayType`
- `DoubleType`

---

## Full Sample Pipeline Example

Here is a complete example:
```scala
val schema = val schema = StructType(Seq(
StructField("temperature", FloatType),
StructField("humidity", FloatType),
StructField("timestamp", StringType),
StructField("metadata", ArrayType(StringType))
))

val df = spark.read.format("netcdf").schema(schema).option("path", "/data/example.nc").load()
df.printSchema() df.show()
``` 

---

## Limitations

- **Schema is Mandatory**: Schema inference is not supported; you must explicitly define the schema.
- **Write Operations**: Currently, writing to NetCDF files is not supported.
- **Common Dimensions**: Variables must share at least one common dimension for proper partitioning.

---

## License

This project is licensed under the **Apache License 2.0**.

---

## Contribution

We welcome contributions! Feel free to create an issue or submit a pull request on GitHub to suggest improvements or fix issues.
```
