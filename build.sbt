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

version := "0.3.0"

scalaVersion := "2.11.8"
sparkVersion := "2.2.0"

sparkComponents ++= Seq("sql")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "provided"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_2.11" + "-" + sparkVersion.value + "_" + module.revision + "." + artifact.extension
}

// All Spark Packages need a license
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled","-Duser.timezone=GMT")