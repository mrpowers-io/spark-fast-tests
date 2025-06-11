logLevel := Level.Warn

resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")

resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.11.1")

addSbtPlugin("org.typelevel" % "laika-sbt" % "1.2.0")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.3")
