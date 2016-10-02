name := "IdentifyCassandraLargePartition"

version := "1.0"

scalaVersion := "2.10.4"

val snapshot = "1.4.1"
val spark = "org.apache.spark"

libraryDependencies ++= Seq("com.datastax.spark" %% "spark-cassandra-connector" % snapshot, //%% adds project scala version to artifact name,
  spark %% "spark-sql" % snapshot,
  spark %% "spark-hive" % snapshot,
  spark %% "spark-core" % snapshot,
  "com.esotericsoftware.yamlbeans" % "yamlbeans" % "1.08")

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

mainClass in assembly := Some("usps.iv.ops.Main")

assemblyMergeStrategy in assembly := {
  case x if x.startsWith("META-INF") => MergeStrategy.discard
  case x if x.endsWith(".html") => MergeStrategy.discard
  case x if x.endsWith(".conf") => MergeStrategy.concat //Needed for Akka
  case x if x.contains("slf4j-api") => MergeStrategy.last
  case x => MergeStrategy.first
}