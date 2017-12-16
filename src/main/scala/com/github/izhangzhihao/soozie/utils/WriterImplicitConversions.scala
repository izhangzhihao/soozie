package com.github.izhangzhihao.soozie.utils

import com.github.izhangzhihao.soozie.conversion.Conversion
import com.github.izhangzhihao.soozie.dsl.{ Bundle, Coordinator, WritableApplication, Workflow }
import com.github.izhangzhihao.soozie.writer.implicits._
import com.github.izhangzhihao.soozie.writer._

import scala.util.Try
import scalaxb.CanWriteXML

object WriterImplicitConversions {
  implicit class Stringable[T](val underlying: T) extends AnyVal {
    def toXmlString(postProcessing: XmlPostProcessing,
                    scope: String,
                    namespace: String,
                    elementLabel: String)(implicit $ev: CanWriteXML[T]): String = {
      WriterUtils.generateXml(underlying, scope, namespace, elementLabel, Some(postProcessing))
    }
  }

  implicit def toWriteable[T: CanWriteXML](workflow: Workflow[T]): Writeable[Workflow[T], T] =
    new Writeable(workflow, Conversion(workflow))

  implicit def toWriteable[C: CanWriteXML, W](coordinator: Coordinator[C, W]): Writeable[Coordinator[C, W], C] =
    new Writeable(coordinator, Conversion(coordinator))

  implicit def toWriteable[B: CanWriteXML, C, W](bundle: Bundle[B, C, W]): Writeable[Bundle[B, C, W], B] =
    new Writeable(bundle, Conversion(bundle))

  class Writeable[W <: WritableApplication, T: CanWriteXML](underlying: W, str: Stringable[T]) {
    def toXmlString(postProcessing: XmlPostProcessing = XmlPostProcessing.Default): String = str.toXmlString(
      postProcessing,
      underlying.scope,
      underlying.namespace,
      underlying.elementLabel
    )

    def write(path: String, fileSystemUtils: FileSystemUtils = LocalFileSystemUtils)(implicit writerEv: CanWrite): Try[Unit] = {
      writerEv.write(path, fileSystemUtils)
    }

    def writeJob(path: String,
                 properties: Option[Map[String, String]],
                 fileSystemUtils: FileSystemUtils = LocalFileSystemUtils)(implicit writerEv: CanWrite): Try[Unit] = {
      writerEv.writeJob(path, properties, fileSystemUtils)
    }
  }
}

