enablePlugins(GitVersioning)

scalafmtOnCompile in Compile := true

organization := "com.github.mrpowers"
name := "spark-fast-tests"

version := "0.23.0"
crossScalaVersions := Seq("2.11.12", "2.12.12")
scalaVersion := crossScalaVersions.value.head
val sparkVersion = "2.4.5"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

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

publishTo := sonatypePublishToBundle.value

Global/useGpgPinentry := true
