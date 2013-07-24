name := "play-hbase"

organization := "com.github.lizepeng"

version := "0.1.1"

scalaVersion := "2.10.1"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "CDH4 repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "com.github.lizepeng" %% "play-hbase-coproc" % "0.1.1",
  "play" %% "play" % "2.1.1" % "provided"
)