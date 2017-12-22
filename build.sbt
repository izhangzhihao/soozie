import sbt.ExclusionRule
import sbtrelease._
import ReleaseStateTransformations._
import ReleasePlugin.autoImport._
import sbt.Keys.scalaVersion

val oozieVersion = "4.2.0"
val hadoopVersion = "2.7.3"
val hadoopMiniClusterVersion = "0.1.14"
val rootFolder = "oozie"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2-core" % "4.0.2-291bdf5-20171123131811" % "test",
  "org.apache.oozie" % "oozie-client" % oozieVersion excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.apache.oozie" % "oozie-core" % oozieVersion classifier "tests" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "com.google.guava" % "guava" % "19.0",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6",
  "joda-time" % "joda-time" % "2.9.9",
  "com.github.sakserv" % "hadoop-mini-clusters-oozie" % hadoopMiniClusterVersion % "test",
  "com.github.sakserv" % "hadoop-mini-clusters-hdfs" % hadoopMiniClusterVersion % "test",
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
  "com.github.jacoby6000" %% "retry" % "0.3.0",
  "org.scalaz" %% "scalaz-core" % "7.2.17",
  //"org.scala-lang" % "scala-reflect" % "2.12.4",
)

dependencyOverrides ++= Seq(
  "org.apache.oozie" % "oozie-core" % oozieVersion,
  "org.apache.oozie" % "oozie-tools" % oozieVersion,
  "org.apache.oozie.test" % "oozie-mini" % oozieVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-mapreduce-client-app" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion
).map(_ % "test")

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases",
  "nexus" at "http://repo.hortonworks.com/content/groups/public",
  "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"
)


lazy val root = (project in file(".")).
  enablePlugins(ScalaxbPlugin).
  settings(
    //    ScalaxbPlugin.baseScalaxbSettings,
    name := "soozie",
    organization := "com.github.izhangzhihao",
    scalaVersion := "2.12.4",
    scalaxbContentsSizeLimit in(Compile, scalaxb) := 20,
    scalaxbNamedAttributes in(Compile, scalaxb) := true,
    scalaxbPackageNames in(Compile, scalaxb) := Map(
      uri("uri:oozie:workflow:0.5") -> s"$rootFolder.workflow_0_5",
      uri("uri:oozie:hive-action:0.5") -> s"$rootFolder.hive_0_5",
      uri("uri:oozie:shell-action:0.3") -> s"$rootFolder.shell_0_3",
      uri("uri:oozie:distcp-action:0.2") -> s"$rootFolder.distcp_0_2",
      uri("uri:oozie:email-action:0.2") -> s"$rootFolder.email_0_2",
      uri("uri:oozie:sla:0.2") -> s"$rootFolder.sla_0_2",
      uri("uri:oozie:spark-action:0.1") -> s"$rootFolder.spark_0_1",
      uri("uri:oozie:sqoop-action:0.4") -> s"$rootFolder.sqoop_0_4",
      uri("uri:oozie:ssh-action:0.2") -> s"$rootFolder.ssh_0_2",
      uri("uri:oozie:coordinator:0.4") -> s"$rootFolder.coordinator_0_4",
      uri("uri:oozie:bundle:0.2") -> s"$rootFolder.bundle_0_2"
    )
  )

scalacOptions ++= Seq(
  "-language:postfixOps",
  "-deprecation",                      // Emit warning and location for usages of deprecated APIs.
  "-encoding", "utf-8",                // Specify character encoding used by source files.
  "-explaintypes",                     // Explain type errors in more detail.
  "-feature",                          // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials",            // Existential types (besides wildcard types) can be written and inferred
  "-language:experimental.macros",     // Allow macro definition (besides implementation and application)
  "-language:higherKinds",             // Allow higher-kinded types
  "-language:implicitConversions",     // Allow definition of implicit functions called views
  "-unchecked",                        // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
  //"-Xfatal-warnings",                  // Fail the compilation if there are any warnings.
  "-Xfuture",                          // Turn on future language features.
  "-Xlint:adapted-args",               // Warn if an argument list is modified to match the receiver.
  "-Xlint:by-name-right-associative",  // By-name parameter of right associative operator.
  "-Xlint:constant",                   // Evaluation of a constant arithmetic expression results in an error.
  "-Xlint:delayedinit-select",         // Selecting member of DelayedInit.
  "-Xlint:doc-detached",               // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible",               // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any",                  // Warn when a type argument is inferred to be `Any`.
  //"-Xlint:missing-interpolator",       // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Xlint:nullary-unit",               // Warn when nullary methods return Unit.
  "-Xlint:option-implicit",            // Option.apply used implicit view.
  "-Xlint:package-object-classes",     // Class or object defined in package object.
  "-Xlint:poly-implicit-overload",     // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow",             // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align",                // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow",      // A local type parameter shadows a type already in scope.
  "-Xlint:unsound-match",              // Pattern match may not be typesafe.
  "-Yno-adapted-args",                 // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ypartial-unification",             // Enable partial unification in type constructor inference
  "-Ywarn-dead-code",                  // Warn when dead code is identified.
  "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
  "-Ywarn-inaccessible",               // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any",                  // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit",               // Warn when nullary methods return Unit.
  "-Ywarn-numeric-widen",              // Warn when numerics are widened.
  "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
  //"-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
  "-Ywarn-unused:locals",              // Warn if a local definition is unused.
  //"-Ywarn-unused:params",              // Warn if a value parameter is unused.
  //"-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates",            // Warn if a private member is unused.
  "-Ywarn-value-discard",              // Warn when non-Unit expression results are unused.
)

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepTask(assembly),
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) =>
    xs map {
      _.toLowerCase
    } match {
      case "services" :: _ => MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) => MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.discard
    }
  case _ => MergeStrategy.first
}