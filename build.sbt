enablePlugins(GitVersioning)

scalafmtOnCompile in Compile := true

organization := "com.github.mrpowers"
name := "spark-fast-tests"
spName := "MrPowers/spark-fast-tests"

spShortDescription := "Fast tests with Spark"
spDescription := "Test your code quickly"

version := "0.21.1"
crossScalaVersions := Seq("2.11.12", "2.12.10")
scalaVersion := crossScalaVersions.value.head
sparkVersion := "2.4.5"

spAppendScalaVersion := true

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  Artifact.artifactName(sv, module, artifact).replaceAll(s"-${module.revision}", s"-${sparkVersion.value}${module.revision}")
}

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled","-Duser.timezone=GMT")

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))
homepage := Some(url("https://github.com/MrPowers/spark-fast-tests"))
developers ++= List(
  Developer("MrPowers", "Matthew Powers", "@MrPowers", url("https://github.com/MrPowers"))
)
scmInfo := Some(ScmInfo(url("https://github.com/MrPowers/spark-fast-tests"), "git@github.com:MrPowers/spark-fast-tests.git"))

updateOptions := updateOptions.value.withLatestSnapshots(false)

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

publishTo := sonatypePublishToBundle.value

Global/useGpgPinentry := true

// Skip tests during assembly
test in assembly := {}

addArtifact(artifact in (Compile, assembly), assembly)
