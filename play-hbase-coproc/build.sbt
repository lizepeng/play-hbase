name := "play-hbase-coproc"

organization := "com.github.lizepeng"

version := "0.1.1"

scalaVersion := "2.10.1"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.1",
  "org.apache.hbase" % "hbase" % "0.92.1"
    excludeAll(
      ExclusionRule(organization = "org.slf4j"),
      ExclusionRule(organization = "org.jboss.netty")
    ),
  "org.apache.hadoop" % "hadoop-core" % "1.0.2"
)