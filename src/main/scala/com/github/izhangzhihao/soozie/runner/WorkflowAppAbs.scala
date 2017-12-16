package com.github.izhangzhihao.soozie.runner

import com.github.izhangzhihao.soozie.dsl.Workflow
import com.github.izhangzhihao.soozie.utils.ExecutionUtils
import org.apache.oozie.client.{OozieClient, WorkflowJob}

import scala.concurrent.Future
import scalaxb.CanWriteXML

abstract class WorkflowAppAbs[W: CanWriteXML] extends ScoozieApp {
  type Job = WorkflowJob
  import com.github.izhangzhihao.soozie.writer.implicits._
  override lazy val writeResult = workflow.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)

  import com.github.izhangzhihao.soozie.writer.implicits._
  override lazy val executionResult: Future[Job] = for {
    _ <- Future.fromTry(writeResult)
    _ <- Future(logWriteResult())
    job <- ExecutionUtils.run[OozieClient, Job](oozieClient, workflow.getJobProperties(appPath, jobProperties))
  } yield job
  val workflow: Workflow[W]
}