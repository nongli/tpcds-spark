# Spark SQL TPCDS benchmark kit.

## Requirements

This library requires Spark 1.3+

## Linking
You can link against this library in your program at the following coordinates:

### Scala 2.10
```
groupId: com.databricks
artifactId: tpcds-spark_2.10
version: 1.2.0
```
### Scala 2.11
```
groupId: com.databricks
artifactId: tpcds-spark_2.11
version: 1.2.0
```

## Using with Spark shell
This package can be added to  Spark using the `--packages` command line option.  For example, to include it when starting the spark shell:

## Features


## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.