name := """scaflow-common"""

organization := "pl.liosedhel"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies ++= {
  val akkaV = "2.4.2"
  Seq("com.typesafe.akka" %% "akka-actor" % akkaV)
}


scalariformSettings
parallelExecution in Test := false

