name := """scaflow-workers"""

organization := "pl.liosedhel"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers += "Local Ivy Repository" at Path.userHome.asFile.toURI.toURL + ".ivy2/local"

libraryDependencies ++= {
  val akkaV = "2.4.7"
  Seq(
    "pl.liosedhel" %% "scaflow" % "1.0-SNAPSHOT",
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-remote" % akkaV)
}


scalariformSettings
parallelExecution in Test := false

