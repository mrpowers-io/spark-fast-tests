logLevel := Level.Warn

resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")

resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % "1.15")

addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.3")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")
