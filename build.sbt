import AssemblyKeys._

name := "spark-cassandra-tests"

organization := "com.jererc"

version := "0.1.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

mainClass := Some("com.jererc.SparkCassandraTests")

resolvers ++= Seq(
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Maven Repository" at "http://mvnrepository.com/artifact/",
    "Sonatype OSS Repo" at "https://oss.sonatype.org/content/repositories/releases",
    "Sonatype OSS Snapshots Repo" at "http://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype OSS Tools Repo" at "https://oss.sonatype.org/content/groups/scala-tools",
    "Concurrent Maven Repo" at "http://conjars.org/repo",
    "Clojars Repository" at "http://clojars.org/repo",
    "Cloudera Repo" at "http://repository.cloudera.com/artifactory/cloudera-repos/",
    "Twitter Maven Repo" at "http://maven.twttr.com"
)

libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "com.google.guava" % "guava" % "16.0",
    "net.liftweb" %% "lift-json" % "2.5.1",
    "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.0",
    "com.datastax.spark" %% "spark-cassandra-connector-java" % "1.1.0",
    "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.2",
    "org.apache.cassandra" % "cassandra-thrift" % "2.1.2",
    "org.apache.cassandra" % "cassandra-clientutil" % "2.1.2",
    "org.apache.cassandra" % "cassandra-all" % "2.1.2",
    "org.apache.spark" %% "spark-core" % "1.1.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "1.1.0"
)

parallelExecution in Test := false

seq(assemblySettings: _*)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case x if Assembly.isConfigFile(x) =>
      MergeStrategy.concat
    case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
      MergeStrategy.rename
    case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") || ps.last.startsWith("pom.") =>
          MergeStrategy.discard
        case "plexus" :: xs =>
          MergeStrategy.discard
        case "services" :: xs =>
          MergeStrategy.filterDistinctLines
        case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
          MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.last
      }
    case _ => MergeStrategy.last // leiningen build files
  }
}
