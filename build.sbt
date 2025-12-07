name := "IndexedDF"

version := "2.0"

scalaVersion := "2.13.16"

val sparkVersion = "4.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.slf4j" % "slf4j-api" % "2.0.16"
)

Test / fork := true

// runMainでProvidedスコープの依存を含める
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
Compile / runMain := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated
