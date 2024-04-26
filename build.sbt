val scala2_13= "2.13.13"
val scala2_12= "2.12.15"
val sparkVersion = settingKey[String]("Spark version")

inThisBuild(Seq(
  organization := "com.github.zeotuan",
  homepage := Some(url("https://github.com/zeotuan/spark-fast-tests")),
  // Alternatively License.Apache2 see https://github.com/sbt/librarymanagement/blob/develop/core/src/main/scala/sbt/librarymanagement/License.scala
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  scmInfo := Some(ScmInfo(url("https://github.com/zeotuan/spark-fast-tests"), "git@github.com:zeotuan/spark-fast-tests.git")),
  developers := List(
    Developer("MrPowers", "Matthew Powers", "@MrPowers", url("https://github.com/MrPowers")),
    Developer("zeotuan", "Anh Tuan Pham", "zeotuan@gmail.com", url("https://github.com/zeotuan")),
  ),
  ThisBuild / sparkVersion := System.getProperty("spark.testVersion", "3.4.2"),
  crossScalaVersions := {
    sparkVersion.value match {
      case versionRegex("3", m, _) if m.toInt >= 2 => Seq(scala2_12, scala2_13)
      case versionRegex("3", _ , _) => Seq(scala2_12)
    }
  },
  scalaVersion := crossScalaVersions.value.head,
  ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org",
  sonatypeRepository :="https://s01.oss.sonatype.org/service/local"
))

name := "spark-fast-tests"

Compile / scalafmtOnCompile:= true

// TODO: auto parse version
version := "1.3.0"

val versionRegex      = """^(.*)\.(.*)\.(.*)$""".r

enablePlugins(JmhPlugin)

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % "test"

Test / fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled", "-Duser.timezone=GMT")

updateOptions := updateOptions.value.withLatestSnapshots(false)