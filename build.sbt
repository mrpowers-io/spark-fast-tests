val versionRegex = """^(.*)\.(.*)\.(.*)$""".r
val scala2_13    = "2.13.14"
val scala2_12    = "2.12.20"
val sparkVersion = System.getProperty("spark.version", "3.5.3")
val noPublish = Seq(
  (publish / skip) := true,
  publishArtifact  := false
)

inThisBuild(
  List(
    organization := "io.github.zeotuan",
    homepage     := Some(url("https://github.com/mrpowers-io/spark-fast-tests")),
    licenses     := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
    developers ++= List(
      Developer("MrPowers", "Matthew Powers", "@MrPowers", url("https://github.com/MrPowers"))
    ),
    Compile / scalafmtOnCompile := true,
    Test / fork                 := true,
    crossScalaVersions := {
      sparkVersion match {
        case versionRegex("3", m, _) if m.toInt >= 2 => Seq(scala2_12, scala2_13)
        case versionRegex("3", _, _)                 => Seq(scala2_12)
      }
    },
    scalaVersion := crossScalaVersions.value.head
  )
)

enablePlugins(GitVersioning)

lazy val commonSettings = Seq(
  javaOptions ++= {
    Seq("-Xms512M", "-Xmx2048M", "-Duser.timezone=GMT") ++ (if (System.getProperty("java.version").startsWith("1.8.0"))
                                                              Seq("-XX:+CMSClassUnloadingEnabled")
                                                            else Seq.empty)
  },
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.scalatest"    %% "scalatest" % "3.2.18"     % "test"
  )
)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    moduleName                             := "spark-fast-tests",
    name                                   := moduleName.value,
    Compile / packageSrc / publishArtifact := true,
    Compile / packageDoc / publishArtifact := true
  )

lazy val benchmarks = (project in file("benchmarks"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.openjdk.jmh" % "jmh-generator-annprocess" % "1.37" // required for jmh IDEA plugin. Make sure this version matches sbt-jmh version!
    ),
    name := "benchmarks"
  )
  .settings(noPublish)
  .enablePlugins(JmhPlugin)

lazy val docs = (project in file("docs"))
  .dependsOn(core)
  .enablePlugins(LaikaPlugin)
  .settings(
    name := "docs",
    laikaTheme := {
      import laika.ast.Path.Root
      import laika.helium.Helium
      import laika.helium.config.*

      Helium.defaults.site
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
    },
    laikaIncludeAPI := true,
    laikaExtensions ++= {
      import laika.config.SyntaxHighlighting
      import laika.format.Markdown
      Seq(Markdown.GitHubFlavor, SyntaxHighlighting)
    },
    Laika / sourceDirectories := Seq((ThisBuild / baseDirectory).value / "docs")
  )
  .settings(noPublish)

lazy val root = (project in file("."))
  .settings(
    name := "spark-fast-tests-root",
    commonSettings,
    noPublish
  )
  .aggregate(core, benchmarks, docs)

scmInfo := Some(ScmInfo(url("https://github.com/mrpowers-io/spark-fast-tests"), "git@github.com:MrPowers/spark-fast-tests.git"))

updateOptions := updateOptions.value.withLatestSnapshots(false)