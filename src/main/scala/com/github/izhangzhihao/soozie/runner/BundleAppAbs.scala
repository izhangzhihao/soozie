package com.github.izhangzhihao.soozie.runner

import com.github.izhangzhihao.soozie.dsl.Bundle
import com.github.izhangzhihao.soozie.utils.ExecutionUtils
import org.apache.oozie.client.OozieClient

import scala.concurrent.Future
import scalaxb.CanWriteXML

abstract class BundleAppAbs[B: CanWriteXML, C: CanWriteXML, W: CanWriteXML] extends SoozieApp {
  type Job = org.apache.oozie.client.Job
  import com.github.izhangzhihao.soozie.writer.implicits._
  override lazy val writeResult = bundle.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)

  override lazy val executionResult: Future[Job] = for {
    _ <- Future.fromTry(writeResult)
    _ <- Future(logWriteResult())
    job <- ExecutionUtils.run[OozieClient, Job](oozieClient, bundle.getJobProperties(appPath, jobProperties))
  } yield job
  val bundle: Bundle[B, C, W]
}
