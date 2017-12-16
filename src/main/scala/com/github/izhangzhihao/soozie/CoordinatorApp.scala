package com.github.izhangzhihao.soozie

import com.github.izhangzhihao.soozie.dsl.Coordinator
import com.github.izhangzhihao.soozie.runner.CoordinatorAppAbs
import com.github.izhangzhihao.soozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}
import scalaxb.CanWriteXML

class CoordinatorApp[C: CanWriteXML, W: CanWriteXML](override val coordinator: Coordinator[C, W],
                                                     oozieUrl: String,
                                                     override val appPath: String,
                                                     override val fileSystemUtils: FileSystemUtils,
                                                     override val properties: Option[Map[String, String]] = None,
                                                     override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
  extends CoordinatorAppAbs[C, W] {
  override val oozieClient: OozieClient = new OozieClient(oozieUrl)

  implicit override val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global

  executionResult.onComplete {
    case Success(_) => println(ScoozieConfig.successMessage)
    case Failure(e) => println(s"Application failed with the following error: ${e.getMessage}")
  }

  import scala.concurrent.duration._

  Await.result(executionResult, 5.minutes)
}