organization := "com.sclasen"

name := "akka-persistence-dynamodb"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.3"

parallelExecution in Test := false

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.sclasen" %% "spray-dynamodb" % "0.2.4-SNAPSHOT" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.0-RC2" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3.0-RC2" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

libraryDependencies += "commons-io" % "commons-io" % "2.4" % "test"

parallelExecution in Test := false