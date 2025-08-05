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
    organization := "com.github.mrpowers",
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

// Automatically copy README.md from the root to docs/README.md before generating the site
lazy val copyRootReadme = taskKey[Unit]("Copy root README.md to docs/about/README.md")

lazy val docs = (project in file("docs"))
  .dependsOn(core)
  .enablePlugins(LaikaPlugin)
  .settings(
    name := "docs",
    copyRootReadme := {
      import java.nio.file.{Files, StandardCopyOption}
      val rootReadme = baseDirectory.value.getParentFile.toPath.resolve("README.md")
      val aboutDir   = baseDirectory.value.toPath.resolve("about")
      val docsReadme = aboutDir.resolve("README.md")

      Files.createDirectories(aboutDir)
      Files.copy(rootReadme, docsReadme, StandardCopyOption.REPLACE_EXISTING)
      println(s"Copied ${rootReadme.toAbsolutePath} to ${docsReadme.toAbsolutePath}")

      // Copy API documentation to the docs directory
      val apiDocs      = (core / Compile / doc).value.toPath
      val targetApiDir = baseDirectory.value.toPath.resolve("api")

      if (Files.exists(targetApiDir)) {
        println(s"Removing existing API documentation directory: ${targetApiDir.toAbsolutePath}")
        Files.walk(targetApiDir).sorted(java.util.Comparator.reverseOrder()).forEach(f => Files.delete(f))
      }

      Files.createDirectories(targetApiDir)

      println("Copying API documentation to " + targetApiDir.toAbsolutePath)
      Files
        .walk(apiDocs)
        .forEach { source =>
          val target = targetApiDir.resolve(apiDocs.relativize(source))
          if (Files.isDirectory(source)) {
            Files.createDirectories(target)
          } else {
            Files.createDirectories(target.getParent)
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
          }
        }
    },
    laikaSite := (laikaSite dependsOn copyRootReadme dependsOn (core / Compile / doc)).value,
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
