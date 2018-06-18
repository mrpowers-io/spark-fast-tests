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