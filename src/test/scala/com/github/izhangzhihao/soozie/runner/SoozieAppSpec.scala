package com.github.izhangzhihao.soozie.runner

import com.github.izhangzhihao.soozie.dsl._
import com.github.izhangzhihao.soozie.jobs.{ShellJob, ShellScript}
import com.github.izhangzhihao.soozie.utils.OozieClientLike
import com.github.izhangzhihao.soozie.writer.FileSystemUtils
import org.apache.hadoop.fs.Path
import org.apache.oozie.LocalOozieClient
import org.apache.oozie.client.{Job, WorkflowJob}
import org.joda.time.{DateTime, DateTimeZone}
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import scala.concurrent.Await
import scala.util.{Failure, Try}
import oozie.distcp.`package`._

class SoozieAppSpec extends Specification with BeforeAfterAll with TestHdfsProvider with TestOozieClientProvider {

  args(skipAll = true)

  "Soozie Application" should {
    "run a workflow application successfully" in {
      val appPath: Path = new Path(fs.getHomeDirectory, "testAppWorkflow")

      val soozieApp = new TestWorkflowApp(
        workflow = Fixtures.workflow("test-workflow"),
        oozieClient = oozieClient,
        appPath = appPath.toString,
        fileSystemUtils = new HdfsFilesystemUtils(fs)
      )

      soozieApp.main(Array.empty)

      import scala.concurrent.duration._
      val result = Await.result(soozieApp.executionResult, 30.second)

      result.getAppName must_== "test-workflow"
    }.pendingUntilFixed("https://github.com/sbt/sbt/issues/2964")

    "run a coordinator application successfully" in {
      val appPath: Path = new Path(fs.getHomeDirectory, "testAppCoordinator")

      val soozieApp = new TestCoordinatorApp(
        coordinator = Fixtures.coordinator("test-coordinator"),
        oozieClient = oozieCoordClient,
        appPath = appPath.toString,
        fileSystemUtils = new HdfsFilesystemUtils(fs)
      )

      soozieApp.main(Array.empty)

      import scala.concurrent.duration._
      val result = Await.result(soozieApp.executionResult, 90.second)

      result.getAppName must_== "test-coordinator"
    }.pendingUntilFixed("https://github.com/sbt/sbt/issues/2964")

    "run a bundle application successfully" in {
      val appPath: Path = new Path(fs.getHomeDirectory, "testAppBundle")

      val soozieApp = new TestBundleApp(
        bundle = Fixtures.bundle("test-bundle"),
        oozieClient = oozieCoordClient,
        appPath = appPath.toString,
        fileSystemUtils = new HdfsFilesystemUtils(fs)
      )

      soozieApp.main(Array.empty)

      import scala.concurrent.duration._
      val result = Await.result(soozieApp.executionResult, 90.seconds)

      result.getAppName must_== "testAppBundle"
    }.pendingUntilFixed("Currently can't because Oozie... LocalTest doesn't support bundles")

    "should catch unexpected errors" in {
      val appPath: Path = new Path(fs.getHomeDirectory, "testAppWorkflowWithUnexpectedError")

      val soozieApp = new TestWorkflowApp(
        workflow = Fixtures.workflowWithErrorOnSubmit("test-workflow"),
        oozieClient = oozieClient,
        appPath = appPath.toString,
        fileSystemUtils = new HdfsFilesystemUtils(fs)
      )

      soozieApp.main(Array.empty)

      import scala.concurrent.duration._
      val result = Try(Await.result(soozieApp.executionResult, 30.second))

      result.isFailure must_== true
      result.failed.get.getMessage must_== "org.apache.oozie.DagEngineException: E0701: XML schema error, cvc-pattern-valid: Value '</' is not facet-valid with respect to pattern '([a-zA-Z_]([\\-_a-zA-Z0-9])*){1,39}' for type 'IDENTIFIER'."
    }.pendingUntilFixed("https://github.com/sbt/sbt/issues/2964")

    "should not submit job if writing has failed" in {
      val appPath: Path = new Path(fs.getHomeDirectory, "testAppWorkflowWithWriteError")

      val fileSystemUtils = new FileSystemUtils {
        override def writeTextFile(path: String, text: String): Try[Unit] = Failure(new Exception("a write error"))

        override def makeDirectory(path: String): Try[Unit] = Failure(new Exception("a write error"))
      }

      val soozieApp = new TestWorkflowApp(
        workflow = Fixtures.workflow("test-workflow"),
        oozieClient = oozieClient,
        appPath = appPath.toString,
        fileSystemUtils = fileSystemUtils
      )

      soozieApp.main(Array.empty)

      import scala.concurrent.duration._
      val result = Try(Await.result(soozieApp.executionResult, 30.second))

      result.isFailure must_== true
      result.failed.get.getMessage must_== "a write error"
    }.pendingUntilFixed("https://github.com/sbt/sbt/issues/2964")
  }
}

object OozieClientLike {
  implicit object OozieClientLikeLocalCoord extends OozieClientLike[LocalOozieClient, Job] {
    def getJobInfo(oozieClient: LocalOozieClient, jobId: String): Job = oozieClient.getCoordJobInfo(jobId)
  }
}

object Fixtures {
  def workflow(name: String) = {
    val end = End dependsOn Start
    Workflow(name, end)
  }

  // Currently this throws an xml parse error when submitting the job to oozie. Ideally Soozie should catch this at
  // compile time.
  def workflowWithErrorOnSubmit(name: String) = {
    val end = End dependsOn ShellJob("</", Right(ShellScript("this script throws an error"))).dependsOn(Start)
    Workflow(name, end)
  }

  def coordinator(name: String) = {
    Coordinator(
      name = name,
      workflow = workflow(s"${name}_workflow"),
      timezone = DateTimeZone.forID("GMT"),
      start = DateTime.now().toDateTime(DateTimeZone.forID("GMT")),
      end = DateTime.now().plusHours(1).toDateTime(DateTimeZone.forID("GMT")),
      frequency = Hours(1),
      configuration = Nil,
      workflowPath = None
    )
  }

  def bundle(name: String) = {
    Bundle(
      name = name,
      coordinators = List(CoordinatorDescriptor("my-coord", coordinator("my-coord"))),
      kickoffTime = Left[DateTime, String](DateTime.now())
    )
  }
}