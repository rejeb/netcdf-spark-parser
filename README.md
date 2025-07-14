# NetCDF Spark Parser

[![GitHub stars](https://img.shields.io/github/stars/rejeb/netcdf-spark-parser)](https://github.com/rejeb/netcdf-spark-parser/stargazers)
[![License](https://img.shields.io/github/license/rejeb/netcdf-spark-parser)](https://github.com/rejeb/netcdf-spark-parser/blob/main/LICENSE)
[![Scala](https://img.shields.io/badge/Java-11-blue)](https://www.java.com/fr/)
[![Scala](https://img.shields.io/badge/Scala-2.12%2F2.13-red)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.x-orange)](https://spark.apache.org/)

A Spark connector for efficiently parsing and reading **NetCDF** files at scale using **Apache Spark**. 
This project leverages the **DataSource V2** API to integrate NetCDF file reading in a distributed and performant way.
This parser uses [NetCDF Java](https://www.unidata.ucar.edu/software/netcdf-java/) to read data from netcdf files.

---
## 🚀 Features

- **Custom Schema Support**: Define the schema for NetCDF variables.
- **Partition Handling**: Automatically manages partitions for large netcdf files.
- **Scalable Performance**: Optimized for distributed computing with Spark.
- **Storage Compatibility**: This connector supports reading NetCDF files from:
    - Local file systems (tested).
    - Amazon S3, see [Dataset URLs](https://docs.unidata.ucar.edu/netcdf-java/5.6/userguide/dataset_urls.html) for configuration (tested).
  
---

## 📋 Requirements

- **Java**: Version 11+
- **Apache Spark**: Version 3.5.x
- **Scala**: Version 2.12,2.13
- **Dependency Management**: SBT, Maven, or similar
- **Unidata repository**: Add Unidata repository, see [Using netCDF-Java Maven Artifacts](https://docs.unidata.ucar.edu/netcdf-java/current/userguide/using_netcdf_java_artifacts.html)

---

## 🧰 Use Cases

- Transform multi dimensional data to tabular form.
- Processing climate and oceanographic data.
- Analyzing multi-dimensional scientific datasets.
- Batch processing of NetCDF files.

## 📖 Usage

Loading data from a NetCDF file into a DataFrame requires that the variables to extract share at least one common dimension.

### Add Dependency to Your Project

To integrate the **NetCDF Spark** connector into your project, add the following dependency to your preferred build tool configuration.
#### Using SBT
Add the following line to your file: `build.sbt`
``` scala
libraryDependencies += "io.github.rejeb" %% "netcdf-spark-parser" % "1.0.0"
```
#### Using Maven
Include the following dependency in the section of your file: `<dependencies>``pom.xml`
``` xml
<dependency>
    <groupId>io.github.rejeb</groupId>
    <artifactId>netcdf-spark-parser_2.13</artifactId>
    <version>1.0.0</version>
</dependency>
```
> **Note**: Change `_2.13` to `_2.12` if your project uses Scala 2.12 instead of 2.13.
>

#### Using Gradle
For Gradle, add this dependency to the `dependencies` block of your file: `build.gradle`
``` groovy
dependencies {
    implementation 'io.github.rejeb:netcdf-spark-parser_2.13:1.0.0'
}
```
> **Hint**: Ensure that the Scala version in the artifact matches your project setup (e.g., `_2.12` or `_2.13`).
>

---

### Define Your NetCDF Schema

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

### Load NetCDF Files

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

### Configuration Options

| Option              | Description                                           | Required | Default       |
|---------------------|-------------------------------------------------------|----------|---------------|
| `path`              | Path to the NetCDF file                               | Yes      | None          |
| `partition.size`     | Rows per partition to optimize parallelism            | No       | 20,000 rows   |
| `dimensions.to.ignore` | Comma-separated list of dimensions to ignore          | No       | None          |

Example with options:

```scala
val df = spark
        .read
        .format("netcdf")
        .schema(schema)
        .option("path", "/path/to/file.nc")
        .option("partition.size", 50000)
        .option("dimensions.to.ignore", "dim1,dim2")
        .load()
``` 

---

### Full Sample Pipeline Example

Here is a complete example:
```scala
val schema = val schema = StructType(Seq(
StructField("temperature", FloatType),
StructField("humidity", FloatType),
StructField("timestamp", StringType),
StructField("metadata", ArrayType(StringType))
))

val df = spark
        .read
        .format("netcdf")
        .schema(schema)
        .option("path", "/data/example.nc")
        .load()

df.printSchema() df.show()
``` 

---

## ⚠️ Limitations

- **Schema inference**: Schema inference is not supported; you must explicitly define the schema.
- **Write Operations**: Currently, writing to NetCDF files is not supported.
- **Common Dimensions**: Too many shared dimensions, or a large Cartesian product between them, 
can cause the parser to fail during partitioning and data reading.

---

## 🤝 Contributing

Contributions are welcome! To contribute:

1. Fork the project
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -am 'Add my feature'`)
4. Push to your branch (`git push origin feature/my-feature`)
5. Create a Pull Request

---

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

