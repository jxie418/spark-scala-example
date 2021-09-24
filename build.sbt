name := "hellospark"
version := "1.0"
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2" % "compile"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" % "compile"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.2" % "compile"

libraryDependencies ++= Seq(
  "com.google.cloud.datastore" % "datastore-v1-proto-client" % "1.6.0",
  "com.google.protobuf" % "protobuf-java-util" % "3.6.1",
  "com.google.api.grpc" % "proto-google-cloud-datastore-v1" % "0.1.27",
  "com.google.apis" % "google-api-services-pubsub" % "v1-rev355-1.22.0",
  "com.google.cloud.bigdataoss" % "util" %"1.6.0",
  "com.google.http-client" % "google-http-client-jackson" % "1.22.0",
  "com.dexcom" %% "bulk_data_parser" % "0.5.0-SNAPSHOT"
)
assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
