name := "play-hbase-coproc"

organization := "com.github.lizepeng"

version := "0.1.1"

scalaVersion := "2.10.1"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "CDH4 repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.1",
  "org.apache.hbase" % "hbase" % "0.94.2-cdh4.2.0"
    excludeAll(
      ExclusionRule(organization = "org.slf4j"),
      ExclusionRule(organization = "org.jboss.netty")
    ),
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.2.0"
)