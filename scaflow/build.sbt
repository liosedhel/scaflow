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
    "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.9")
}

scalariformSettings
coverageEnabled in Test := true
coverageMinimum := 80
coverageFailOnMinimum := true
parallelExecution in Test := false

useGpg := true

pgpSecretRing := file("~/.gnupg/secring.gpg")

usePgpKeyHex("312AA80F")

// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "liosedhel"

// To sync with Maven central, you need to supply the following information:
pomExtra in Global := {
  <url>https://github.com/liosedhel/scaflow</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/liosedhel/scaflow</connection>
      <developerConnection>scm:git:git@github.com:liosedhel/scaflow.git</developerConnection>
      <url>github.com/liosedhel/scaflow</url>
    </scm>
    <developers>
      <developer>
        <id>liosedhel</id>
        <name>Krzysztof Borowski</name>
        <url>liosedhel.pl</url>
      </developer>
    </developers>
}



