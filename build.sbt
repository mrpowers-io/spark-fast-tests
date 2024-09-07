enablePlugins(GitVersioning)
Compile / scalafmtOnCompile := true

organization := "com.github.mrpowers"
name         := "spark-fast-tests"

version := "1.10.1"

val versionRegex = """^(.*)\.(.*)\.(.*)$""".r

val sparkVersion = settingKey[String]("Spark version")

val scala2_13 = "2.13.13"
val scala2_12 = "2.12.15"
val scala2_11 = "2.11.17"

sparkVersion := System.getProperty("spark.testVersion", "3.5.1")
crossScalaVersions := {
  sparkVersion.value match {
    case versionRegex("3", m, _) if m.toInt >= 2 => Seq(scala2_12, scala2_13)
    case versionRegex("3", _, _)                 => Seq(scala2_12)
  }
}

scalaVersion := crossScalaVersions.value.head

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"
libraryDependencies += "org.scalatest"    %% "scalatest" % "3.2.18"           % "test"

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

Test / fork := true
javaOptions ++= {
  Seq("-Xms512M", "-Xmx2048M", "-Duser.timezone=GMT")  ++ (if (System.getProperty("java.version").startsWith("1.8.0"))
    Seq("-XX:+CMSClassUnloadingEnabled")
  else Seq.empty)
}

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

enablePlugins(LaikaPlugin)

import laika.format.Markdown
import laika.config.SyntaxHighlighting
import laika.ast.Path.Root
import laika.ast.{Image, ExternalTarget}
import laika.helium.config._
import laika.helium.Helium

laikaTheme := Helium.defaults.site
  .landingPage(
    title = Some("Spark Fast Tests"),
    subtitle = Some("Unit testing your Apache Spark application"),
    latestReleases = Seq(
      ReleaseInfo("Latest Stable Release", "1.0.0")
    ),
    license = Some("MIT"),
    titleLinks = Seq(
      VersionMenu.create(unversionedLabel = "Getting Started"),
      LinkGroup.create(
        IconLink.external("https://github.com/mrpowers-io/spark-fast-tests", HeliumIcon.github)
      )
    ),
    linkPanel = Some(
      LinkPanel(
        "Documentation",
        TextLink.internal(Root / "about" / "README.md", "Spark Fast Tests")
      )
    ),
    projectLinks = Seq(
      LinkGroup.create(
        TextLink.internal(Root / "api" / "com" / "github" / "mrpowers" / "spark" / "fast" / "tests" / "index.html", "API (Scaladoc)")
      )
    ),
    teasers = Seq(
      Teaser("Fast", "Handle small dataframes effectively and provide column assertions"),
      Teaser("Flexible", "Works fine with scalatest, uTest, munit")
    )
  )
  .build

laikaIncludeAPI := true
laikaExtensions ++= Seq(Markdown.GitHubFlavor, SyntaxHighlighting)
Laika / sourceDirectories := Seq((ThisBuild / baseDirectory).value / "docs")
