import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(SpacesAroundMultiImports, false)

name := "spark-fast-tests"

spName := "MrPowers/spark-fast-tests"

spShortDescription := "Fast tests with Spark"

spDescription := "Test your code quickly"

scalaVersion := "2.11.8"
sparkVersion := "2.3.0"

version := "0.7.0"

sparkComponents ++= Seq("sql")

libraryDependencies += "com.lihaoyi" %% "utest" % "0.6.3" % "test"
testFrameworks += new TestFramework("utest.runner.Framework")

libraryDependencies += "mrpowers" % "spark-daria" % "2.3.0_0.18.0" % "test"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${artifact.name}_${module.revision}.${artifact.extension}"
}

// All Spark Packages need a license
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled","-Duser.timezone=GMT")
