package com.github.izhangzhihao.soozie.writer

import com.github.izhangzhihao.soozie.ScoozieConfig._
import com.github.izhangzhihao.soozie.dsl.{Bundle, Coordinator, Workflow}
import com.github.izhangzhihao.soozie.jobs.ShellScriptDescriptor
import com.github.izhangzhihao.soozie.utils.WriterImplicitConversions._
import com.github.izhangzhihao.soozie.utils.WriterUtils._
import org.apache.oozie.client.OozieClient

import scala.util.Try
import scalaxb.CanWriteXML

package object implicits {

  trait CanWrite {
    def toXml(xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): String

    def write(path: String,
              fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
              xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit]

    def writeJob(path: String,
                 properties: Option[Map[String, String]] = None,
                 fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
                 xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit]

    def getJobProperties(path: String,
                         properties: Option[Map[String, String]] = None): Map[String, String]

    protected def writeShellScripts(path: String,
                                    shellActions: List[ShellScriptDescriptor],
                                    fileSystemUtils: FileSystemUtils = LocalFileSystemUtils) = {
      import com.github.izhangzhihao.soozie.ScoozieConfig._
      import com.github.izhangzhihao.soozie.utils.SeqImplicits._

      val pathBuilder: PathBuilder = new PathBuilder(path)
      import pathBuilder._

      for {
        _ <- fileSystemUtils.makeDirectory(getScriptsFolderPath)
        _ <- sequenceTry(shellActions.map(descriptor =>
          fileSystemUtils
            .writeTextFile(getScriptFilePath(s"${descriptor.name}.$scriptExtension"), descriptor.script)))
      } yield ()
    }
  }

  implicit class WorkflowWriter[W: CanWriteXML](underlying: Workflow[W]) extends CanWrite {
    override def write(path: String,
                       fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
                       xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      fileSystemUtils.writeTextFile(path, underlying.toXmlString(xmlPostProcessing))
    }

    override def writeJob(path: String,
                          properties: Option[Map[String, String]] = None,
                          fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
                          xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      val pathBuilder: PathBuilder = new PathBuilder(path)
      import pathBuilder._

      val workflowFilename = withXmlExtension(underlying.name)

      import com.github.izhangzhihao.soozie.utils.PropertyImplicits._

      val propertiesString = (getJobProperties(path, properties)).toProperties.toWritableString

      for {
        _ <- fileSystemUtils.makeDirectory(getTargetFolderPath)
        _ <- fileSystemUtils.makeDirectory(getWorkflowFolderPath)
        _ <- fileSystemUtils.writeTextFile(getPropertiesFilePath, propertiesString)
        _ <- fileSystemUtils.writeTextFile(getWorkflowFilePath(workflowFilename), underlying.toXmlString(xmlPostProcessing))
        _ <- writeShellScripts(path, findShellActions(underlying), fileSystemUtils)
      } yield ()
    }

    override def getJobProperties(path: String,
                                  properties: Option[Map[String, String]] = None): Map[String, String] = {
      val pathBuilder: PathBuilder = new PathBuilder(path)

      buildProperties(
        path,
        OozieClient.APP_PATH,
        s"/$workflowFolderName/${withXmlExtension(underlying.name)}",
        (properties ++ Some(getShellActionProperties(underlying))).headOption)
    }

    override def toXml(xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default) = underlying.toXmlString(xmlPostProcessing)
  }

  implicit class CoordinatorWriter[C: CanWriteXML, W: CanWriteXML](underlying: Coordinator[C, W]) extends CanWrite {
    override def write(path: String,
                       fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
                       xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      assertPathIsDefined(underlying.workflowPath, "Workflow", underlying.workflow.name)

      fileSystemUtils.writeTextFile(path, underlying.toXmlString(xmlPostProcessing))
    }

    override def writeJob(path: String,
                          properties: Option[Map[String, String]] = None,
                          fileSystemUtils: FileSystemUtils,
                          xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      assertPathIsEmpty(underlying.workflowPath, "Workflow", underlying.workflow.name)

      val pathBuilder: PathBuilder = new PathBuilder(path)

      import pathBuilder._

      val coordinatorFilename = withXmlExtension(underlying.name)

      val workflowName = underlying.workflow.name
      val workflowPath = getWorkflowFilePath(withXmlExtension(workflowName))

      import com.github.izhangzhihao.soozie.utils.PropertyImplicits._
      val propertiesString = getJobProperties(path, properties).toProperties.toWritableString

      for {
        _ <- fileSystemUtils.makeDirectory(getTargetFolderPath)
        _ <- fileSystemUtils.makeDirectory(getWorkflowFolderPath)
        _ <- fileSystemUtils.makeDirectory(getCoordinatorFolderPath)
        _ <- fileSystemUtils.writeTextFile(getPropertiesFilePath, propertiesString)
        _ <- fileSystemUtils.writeTextFile(
          getCoordinatorFilePath(coordinatorFilename),
          underlying.toXmlString(xmlPostProcessing))
        _ <- fileSystemUtils.writeTextFile(workflowPath, underlying.workflow.toXmlString(xmlPostProcessing))
        _ <- writeShellScripts(path, findShellActions(underlying.workflow), fileSystemUtils)
      } yield ()
    }

    override def getJobProperties(path: String,
                                  properties: Option[Map[String, String]] = None): Map[String, String] = {

      val coordinatorFilename = withXmlExtension(underlying.name)
      val workflowName = underlying.workflow.name

      buildProperties(
        rootPath = path,
        applicationProperty = OozieClient.COORDINATOR_APP_PATH,
        applicationPath = s"/$coordinatorFolderName/$coordinatorFilename",
        properties =
          Some(
            properties.getOrElse(Map[String, String]()) ++
              getShellActionProperties(underlying.workflow) +
              createPathProperty(workflowName, workflowFolderName)))
    }

    override def toXml(xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default) =
      underlying.toXmlString(xmlPostProcessing)
  }

  implicit class BundleWriter[B: CanWriteXML, C: CanWriteXML, W: CanWriteXML, A](underlying: Bundle[B, C, W]) extends CanWrite {
    override def write(path: String,
                       fileSystemUtils: FileSystemUtils = LocalFileSystemUtils,
                       xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      underlying.coordinators.foreach(descriptor => {
        assertPathIsDefined(descriptor.path, "Coordinator", descriptor.coordinator.name)
      })

      fileSystemUtils.writeTextFile(path, underlying.toXmlString(xmlPostProcessing))
    }

    override def writeJob(path: String,
                          properties: Option[Map[String, String]] = None,
                          fileSystemUtils: FileSystemUtils,
                          xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default): Try[Unit] = {
      underlying.coordinators.foreach(descriptor => {
        assertPathIsEmpty(descriptor.path, "Coordinator", descriptor.coordinator.name)
        assertPathIsEmpty(descriptor.coordinator.workflowPath, "Workflow", descriptor.coordinator.workflow.name)
      })

      val pathBuilder: PathBuilder = new PathBuilder(path)

      import com.github.izhangzhihao.soozie.utils.SeqImplicits._
      import pathBuilder._

      val bundleFileName = withXmlExtension(underlying.name)

      import com.github.izhangzhihao.soozie.utils.PropertyImplicits._
      val propertiesString = getJobProperties(path, properties).toProperties.toWritableString

      def writeWorkflows() = {
        for {
          _ <- fileSystemUtils.makeDirectory(getWorkflowFolderPath)
          _ <- sequenceTry(underlying.coordinators.map(descriptor =>
            fileSystemUtils
              .writeTextFile(
                getWorkflowFilePath(withXmlExtension(descriptor.coordinator.workflow.name)),
                descriptor.coordinator.workflow.toXml(xmlPostProcessing))))
        } yield ()
      }

      def writeShellScripts() = {
        for {
          _ <- fileSystemUtils.makeDirectory(getScriptsFolderPath)
          _ <- this.writeShellScripts(
            path,
            underlying.coordinators.flatMap(descriptor => findShellActions(descriptor.coordinator.workflow)),
            fileSystemUtils)
        } yield ()
      }

      def writeCoordinators() = {
        for {
          _ <- fileSystemUtils.makeDirectory(getCoordinatorFolderPath)
          _ <- sequenceTry(underlying.coordinators.map(descriptor =>
            fileSystemUtils
              .writeTextFile(
                getCoordinatorFilePath(withXmlExtension(descriptor.coordinator.name)),
                descriptor.coordinator.toXml(xmlPostProcessing))))
        } yield ()
      }

      for {
        _ <- fileSystemUtils.makeDirectory(getTargetFolderPath)
        _ <- fileSystemUtils.makeDirectory(getBundleFolderPath)
        _ <- fileSystemUtils.writeTextFile(getPropertiesFilePath, propertiesString)
        _ <- fileSystemUtils.writeTextFile(getBundleFilePath(bundleFileName), underlying.toXml(xmlPostProcessing))
        _ <- writeCoordinators()
        _ <- writeWorkflows()
        _ <- writeShellScripts()
      } yield ()
    }

    override def toXml(xmlPostProcessing: XmlPostProcessing = XmlPostProcessing.Default) = underlying.toXmlString(xmlPostProcessing)

    override def getJobProperties(path: String,
                                  properties: Option[Map[String, String]] = None): Map[String, String] = {
      val bundleFileName = withXmlExtension(underlying.name)

      val pathProperties = underlying.coordinators.flatMap(descriptor => {
        List(
          createPathProperty(descriptor.coordinator.workflow.name, workflowFolderName),
          createPathProperty(descriptor.coordinator.name, coordinatorFolderName)
        ) ++ getShellActionProperties(descriptor.coordinator.workflow)
      }).toSet

      buildProperties(
        rootPath = path,
        applicationProperty = OozieClient.BUNDLE_APP_PATH,
        applicationPath = s"/$bundleFolderName/$bundleFileName",
        properties = Some(properties.getOrElse(Map[String, String]()) ++ pathProperties))
    }
  }

}