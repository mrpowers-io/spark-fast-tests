scalafmtOnCompile in Compile := true

organization := "com.github.mrpowers"
name := "spark-fast-tests"
spName := "MrPowers/spark-fast-tests"

spShortDescription := "Fast tests with Spark"
spDescription := "Test your code quickly"

version := "0.20.1"
crossScalaVersions := Seq("2.11.12", "2.12.7")
scalaVersion := "2.11.12"
sparkVersion := "2.4.3"

spAppendScalaVersion := true

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"

libraryDependencies += "com.lihaoyi" %% "utest" % "0.6.3" % "test"
testFrameworks += new TestFramework("com.github.mrpowers.spark.fast.tests.CustomFramework")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "-" + module.revision + "." + artifact.extension
}

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled","-Duser.timezone=GMT")

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

updateOptions := updateOptions.value.withLatestSnapshots(false)