ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.11.12"

val scalaSpark3 = "2.11.12"
val sparkVersion3 = "2.4.5"

val generalDependencyOverrides = Seq(
  "com.github.luben" % "zstd-jni" % "1.5.5-4",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.1",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.15.1",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.1"
)

val generalLibraryDependencies = Seq(
  "com.google.cloud.spark" %% "spark-bigquery" % "0.29.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion3 % "provided",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.17" % "provided",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "12.8.1.jre11" % "provided",
  "com.typesafe" % "config" % "1.4.0"
)

val spark3Dependencies = Seq(
  "org.apache.spark" %% "spark-hive" % sparkVersion3 % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion3 % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion3 % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion3 % "provided",
)

lazy val commonSettings = Seq(
  name := "GCP_Migration",
  version := "0.1",
  dependencyOverrides ++= generalDependencyOverrides,
  libraryDependencies ++= generalLibraryDependencies,
  Compile / mainClass := Some("com.ejada.main.DataLikeMain"),
  assembly / assemblyJarName := "DataLike.jar",
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
)

lazy val root = (project in file("."))
  .settings(commonSettings *)
  .settings(
    scalaVersion := scalaSpark3,
    libraryDependencies ++= spark3Dependencies,
  )