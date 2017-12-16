package com.github.izhangzhihao.soozie.runner

import com.github.izhangzhihao.soozie.dsl.Coordinator
import com.github.izhangzhihao.soozie.utils.ExecutionUtils
import org.apache.oozie.client.OozieClient

import scala.concurrent.Future
import scalaxb.CanWriteXML

abstract class CoordinatorAppAbs[C: CanWriteXML, W: CanWriteXML] extends ScoozieApp {
  val coordinator: Coordinator[C, W]

  type Job = org.apache.oozie.client.Job

  import com.github.izhangzhihao.soozie.writer.implicits._
  override lazy val writeResult = coordinator.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)

  override lazy val executionResult: Future[Job] = for{
    _ <- Future.fromTry(writeResult)
    _ <- Future(logWriteResult())
    job <- ExecutionUtils.run[OozieClient, Job](oozieClient, coordinator.getJobProperties(appPath, jobProperties))
  } yield job
}
