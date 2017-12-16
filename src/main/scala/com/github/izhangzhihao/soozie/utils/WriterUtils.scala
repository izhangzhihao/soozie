package com.github.izhangzhihao.soozie.utils

import com.github.izhangzhihao.soozie.ScoozieConfig
import com.github.izhangzhihao.soozie.conversion.Flatten
import com.github.izhangzhihao.soozie.dsl.{Node, Workflow}
import com.github.izhangzhihao.soozie.jobs.{ShellScriptDescriptor, ShellJob}

import com.github.izhangzhihao.soozie.writer.XmlPostProcessing

import scala.xml.NodeSeq
import scalaxb.CanWriteXML
import scalaxb.`package`._

object WriterUtils {
  def buildProperties(rootPath: String,
                      applicationProperty: String,
                      applicationPath: String,
                      properties: Option[Map[String, String]] = None): Map[String, String] = {

    properties.getOrElse(Map.empty) ++ Map(
      ScoozieConfig.rootFolderParameterName -> rootPath,
      applicationProperty -> addRootSubstitutionToPath(applicationPath)
    )
  }

  def withXmlExtension(name: String): String = s"$name.${ScoozieConfig.xmlExtension}"

  def generateXml[A: CanWriteXML](xmlObject: A,
                                  scope: String,
                                  namespace: String,
                                  elementLabel: String,
                                  postProcessing: Option[XmlPostProcessing]): String = {

    val defaultScope = scalaxb.toScope(None -> scope)
    val xml = toXML[A](xmlObject, Some(namespace), elementLabel, defaultScope)
    postProcess(xml, postProcessing)
  }

  def postProcess(input: NodeSeq, postProcessing: Option[XmlPostProcessing]): String = {
    val prettyPrinter = new scala.xml.PrettyPrinter(Int.MaxValue, 4)
    val formattedXml = prettyPrinter.formatNodes(input)
    val processedXml = postProcessing match {
      case Some(proccessingRules) =>
        (formattedXml /: proccessingRules.substitutions) ((str, mapping) => str replace(mapping._1, mapping._2))
      case _ => formattedXml
    }
    processedXml
  }

  def assertPathIsEmpty(path: Option[String], applicationType: String, applicationName: String) = assert(
    assertion = path.isEmpty,
    message = s"The path: ${path.get} was provided for the following $applicationType: $applicationName. This must be None to write a job")

  def assertPathIsDefined(path: Option[String], applicationType: String, applicationName: String) = assert(
    assertion = path.isDefined,
    message = s"A path was not defined for the following $applicationType: $applicationName")

  def getShellActionProperties(workflow: Workflow[_]) = findShellActions(workflow)
    .map(descriptor =>
      createPathProperty(descriptor.name, ScoozieConfig.scriptFolderName, ScoozieConfig.scriptExtension))
    .toMap

  def createPathProperty(name: String, folderName: String, extension: String = ScoozieConfig.xmlExtension): (String, String) = {
    val fileName = s"$name.$extension"
    val substitutedPath = addRootSubstitutionToPath(s"/$folderName/$fileName")

    buildPathPropertyName(name) -> substitutedPath
  }

  def addRootSubstitutionToPath(path: String) = "${" + ScoozieConfig.rootFolderParameterName + "}" + path

  def buildPathPropertyName(name: String) = s"${name.replace("-", "_")}_path"

  def findShellActions(workflow: Workflow[_]): List[ShellScriptDescriptor] =
    Flatten(workflow).toList
      .map { case (dependency, _) => dependency.value }
      .collect { case x: Node => x.work }
      .collect { case x: ShellJob[_] => x.descriptor }
      .flatten
}
