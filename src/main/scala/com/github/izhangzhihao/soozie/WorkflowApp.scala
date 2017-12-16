package com.github.izhangzhihao.soozie

import com.github.izhangzhihao.soozie.dsl._
import com.github.izhangzhihao.soozie.runner.WorkflowAppAbs
import com.github.izhangzhihao.soozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}
import scalaxb.CanWriteXML

class WorkflowApp[W: CanWriteXML](override val workflow: Workflow[W],
                                  oozieUrl: String,
                                  override val appPath: String,
                                  override val fileSystemUtils: FileSystemUtils,
                                  override val properties: Option[Map[String, String]] = None,
                                  override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
  extends WorkflowAppAbs[W] {
  override val oozieClient: OozieClient = new OozieClient(oozieUrl)

  implicit override val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global

  executionResult.onComplete {
    case Success(_) => println(SoozieConfig.successMessage)
    case Failure(e) => println(s"Application failed with the following error: ${e.getMessage}")
  }

  import scala.concurrent.duration._

  Await.result(executionResult, 5.minutes)
}
