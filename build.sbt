scalafmtOnCompile in Compile := true

organization := "com.github.mrpowers"
name := "spark-fast-tests"
spName := "MrPowers/spark-fast-tests"

spShortDescription := "Fast tests with Spark"
spDescription := "Test your code quickly"

version := "0.21.1"
crossScalaVersions := Seq("2.11.12", "2.12.10")
scalaVersion := "2.11.12"
sparkVersion := "2.4.5"

spAppendScalaVersion := true

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "-" + module.revision + "." + artifact.extension
}

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled","-Duser.timezone=GMT")

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

updateOptions := updateOptions.value.withLatestSnapshots(false)