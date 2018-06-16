import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(SpacesAroundMultiImports, false)

resolvers += "jitpack" at "https://jitpack.io"

name := "spark-fast-tests"

scalaVersion := "2.11.12"
val sparkVersion = "2.2.1"
val sparkDariaVersion = s"v${sparkVersion}_0.21.0"

version := "0.12.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "com.lihaoyi" %% "utest" % "0.6.3" % "test"
testFrameworks += new TestFramework("com.github.mrpowers.spark.fast.tests.CustomFramework")

libraryDependencies += "com.github.mrpowers" % "spark-daria" % sparkDariaVersion % "test"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled","-Duser.timezone=GMT")

// POM settings for Sonatype
homepage := Some(url("https://github.com/mrpowers/spark-fast-tests/"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/mrpowers/spark-fast-tests/"),
    "git@github.com:mrpowers/spark-fast-tests.git"
  )
)
developers := List(
  Developer(
    "mrpowers",
    "Matthew Powers",
    "matthewkevinpowers@gmail.com",
    url("https://github.com/mrpowers/spark-fast-tests/")
  )
)
licenses += ("MIT", url("http://opensource.org/licenses/MIT"))
publishMavenStyle := true

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

organization := "com.github.mrpowers"
