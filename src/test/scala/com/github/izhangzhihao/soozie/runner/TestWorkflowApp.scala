package com.github.izhangzhihao.soozie.runner

import com.github.izhangzhihao.soozie.{SoozieConfig}
import com.github.izhangzhihao.soozie.dsl.Workflow
import com.github.izhangzhihao.soozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}
import scalaxb.CanWriteXML

class TestWorkflowApp[W: CanWriteXML](override val workflow: Workflow[W],
                                      override val oozieClient: OozieClient,
                                      override val appPath: String,
                                      override val fileSystemUtils: FileSystemUtils,
                                      override val properties: Option[Map[String, String]] = None,
                                      override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
  extends WorkflowAppAbs[W] {

  implicit override val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global

  executionResult.onComplete {
    case Success(_) => println(SoozieConfig.successMessage)
    case Failure(e) => println(s"Application failed with the following error: ${e.getMessage}")
  }
}
