import AssemblyKeys._ // put this at the top of the file

name := "TwitterGraphBuilder"

version := "0.0.1"

scalaVersion := "2.9.2"

// resolvers += "repo.codahale.com" at "http://repo.codahale.com/"

// libraryDependencies += "com.codahale" % "jerkson_2.9.1" % "0.5.0"

libraryDependencies += "junit" % "junit" % "4.8" % "test"

libraryDependencies += "net.minidev" % "json-smart" % "2.0-RC2"

libraryDependencies += "org.spark-project" % "spark-core_2.9.2" % "0.6.0-cdh"

assemblySettings

jarName in assembly := "deps.jar"

test in assembly := {}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter {_.data.getName == "spark-core_2.9.2.jar"}
}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("com", "google", "common",  xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
    case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
    case PathList("org", "objectweb", xs @ _*) => MergeStrategy.first
    case PathList("META-INF", "maven", xs @ _*) => MergeStrategy.discard
      case "application.conf" => MergeStrategy.concat
      case "unwanted.txt"     => MergeStrategy.discard
      case "pom.properties"     => MergeStrategy.discard
      case "about.html"     => MergeStrategy.discard
      case x => old(x)
  }
}
