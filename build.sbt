import AssemblyKeys._ // put this at the top of the file

name := "TwitterGraphBuilder"

version := "0.0.1"

scalaVersion := "2.9.2"

resolvers += "repo.codahale.com" at "http://repo.codahale.com/"

libraryDependencies += "com.codahale" % "jerkson_2.9.1" % "0.5.0"

libraryDependencies += "junit" % "junit" % "4.8" % "test"

// libraryDependencies += "org.spark-project" % "spark-core_2.9.1" % "0.5.1-SNAPSHOT"

assemblySettings

jarName in assembly := "TwitterGraphBuilder.jar"

test in assembly := {}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter {_.data.getName == "spark-core-assembly-0.6.0.jar"}
}

// mainClass in assembly := Some("scala.graphbuilder.twitter.TwitterLDAGraph") 
