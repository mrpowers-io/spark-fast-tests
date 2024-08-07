logLevel := Level.Warn

resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")

resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.3")

addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.3")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")