resolvers += Resolver.typesafeRepo("releases")
resolvers += Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("org.scalaxb" % "sbt-scalaxb" % "1.5.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.3.3")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.7")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")