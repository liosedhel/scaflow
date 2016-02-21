name := """scaflow"""
organization := "pl.liosedhel"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"

resolvers += "Local Ivy Repository" at Path.userHome.asFile.toURI.toURL + ".ivy2/local"

libraryDependencies ++= {
  val akkaV = "2.4.2"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
    "com.typesafe.akka" %% "akka-persistence" % akkaV,
    "com.typesafe.akka" %% "akka-remote" % akkaV,
    "org.iq80.leveldb"            % "leveldb"          % "0.7",
    "org.fusesource.leveldbjni"   % "leveldbjni-all"   % "1.8",
    "org.scalatest" %% "scalatest" % "2.2.6" % "test",
    "org.slf4j" % "slf4j-nop" % "1.7.14",
    "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.9",
    "pl.liosedhel" %% "scaflow-common" % "1.0-SNAPSHOT")
}




scalariformSettings
coverageEnabled in Test := true
coverageMinimum := 80
coverageFailOnMinimum := true
parallelExecution in Test := false

