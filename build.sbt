enablePlugins(GitVersioning)
Compile / scalafmtOnCompile:= true

organization := "com.github.mrpowers"
name := "spark-fast-tests"

version := "1.10.1"

val versionRegex      = """^(.*)\.(.*)\.(.*)$""".r

val sparkVersion = settingKey[String]("Spark version")

val scala2_13= "2.13.13"
val scala2_12= "2.12.15"
val scala2_11= "2.11.17"

sparkVersion := System.getProperty("spark.testVersion", "3.5.1")
crossScalaVersions := {
  sparkVersion.value match {
    case versionRegex("3", m, _) if m.toInt >= 2 => Seq(scala2_12, scala2_13)
    case versionRegex("3", _ , _)                => Seq(scala2_12)
  }
}

scalaVersion := crossScalaVersions.value.head

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % "test"

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

Test / fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled", "-Duser.timezone=GMT")

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))
homepage := Some(url("https://github.com/mrpowers-io/spark-fast-tests"))
developers ++= List(
  Developer("MrPowers", "Matthew Powers", "@MrPowers", url("https://github.com/MrPowers"))
)
scmInfo := Some(ScmInfo(url("https://github.com/mrpowers-io/spark-fast-tests"), "git@github.com:MrPowers/spark-fast-tests.git"))

updateOptions := updateOptions.value.withLatestSnapshots(false)

publishMavenStyle := true

publishTo := sonatypePublishToBundle.value

Global / useGpgPinentry := true