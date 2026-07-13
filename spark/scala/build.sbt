name := "spark-iceberg-bench"
version := "1.0.0"
scalaVersion := "2.13.15"

val sparkVersion = "4.1.2"

// Spark + connector jars are provided by the runtime image (see spark/Dockerfile).
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"            % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided
)

// Thin jar; no assembly needed because all deps are provided at runtime.
Compile / mainClass := Some("com.benchmark.IcebergIngestJob")
