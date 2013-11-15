import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

organization := "es.weso"

name := "WiCompute"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.2"

seq(assemblySettings: _*)

libraryDependencies ++= Seq("junit" % "junit" % "4.8.1" % "test" , 
							"commons-configuration" % "commons-configuration" % "1.7",
							"org.apache.jena" % "apache-jena-libs" % "2.11.0", // excludeAll(ExclusionRule(organization = "org.slf4j")),
							"commons-io" % "commons-io" % "2.4",
							"org.rogach" %% "scallop" % "0.9.1", 
							"org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
							"com.typesafe" % "config" % "1.0.1",
							"org.scalaj" %% "scalaj-collection" % "1.5"
)

// mainClass in assembly := Some("es.weso.wiExtract.Main.class")

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
    case entry => {
      val strategy = mergeStrategy(entry)
      if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
      else strategy
    }
  }
}

// Maven Central Specifics

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/weso/wiFetcher</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>https://github.com/weso/wiCompute</url>
    <connection>scm:git:git@github.com:weso/wiCompute.git</connection>
  </scm>
  <developers>
    <developer>
      <name>Jose Emilio Labra Gayo</name>
      <email>jelabra@gmail.com</email>
      <url>http://www.di.uniovi.es/~labra</url>
      <organization>WESO</organization>
      <organizationUrl>http://www.weso.es</organizationUrl>
      <roles>
        <role>Project Manager</role>
        <role>architect</role>
        <role>developer</role>
        <role>tester</role>
      </roles>
     </developer>
  </developers>)