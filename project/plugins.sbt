logLevel := Level.Warn

resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.7-astraea.1")

resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % "1.15")

addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.3")
