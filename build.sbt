import sbt.ExclusionRule
import sbtrelease._
import ReleaseStateTransformations._
import ReleasePlugin.autoImport._
import sbt.Keys.scalaVersion
import sbtassembly.AssemblyKeys._

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
  "com.github.jacoby6000" %% "retry" % "0.3.0"
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
    name := "soozie",
    organization := "com.github.izhangzhihao.soozie",
    scalaVersion := "2.12.4",
    scalaxbAutoPackages in(Compile, scalaxb) := true,
    scalaxbPackageName in(Compile, scalaxb) := "oozie",
    scalaxbPackageNames in(Compile, scalaxb) := Map(
      uri("uri:oozie:workflow") -> s"$rootFolder.workflow",
      uri("uri:oozie:hive-action") -> s"$rootFolder.hive",
      uri("uri:oozie:shell-action") -> s"$rootFolder.shell",
      uri("uri:oozie:distcp-action") -> s"$rootFolder.distcp",
      uri("uri:oozie:email-action") -> s"$rootFolder.email",
      uri("uri:oozie:sla") -> s"$rootFolder.sla",
      uri("uri:oozie:spark-action") -> s"$rootFolder.spark",
      uri("uri:oozie:sqoop-action") -> s"$rootFolder.sqoop",
      uri("uri:oozie:ssh-action") -> s"$rootFolder.ssh",
      uri("uri:oozie:coordinator") -> s"$rootFolder.coordinator",
      uri("uri:oozie:bundle") -> s"$rootFolder.bundle"
    ),
    scalaxbAsync in(Compile, scalaxb) := true,
  )

scalacOptions ++= Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:higherKinds"
)