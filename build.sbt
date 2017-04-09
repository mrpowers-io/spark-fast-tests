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

version := "0.0.1"

scalaVersion := "2.11.8"
sparkVersion := "2.1.0"

sparkComponents ++= Seq("sql")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_2.11" + "-" + sparkVersion.value + "_" + module.revision + "." + artifact.extension
}

parallelExecution in Test := false

// All Spark Packages need a license
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")