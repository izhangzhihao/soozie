package com.github.izhangzhihao.soozie.writer

import com.github.izhangzhihao.soozie.ScoozieConfig._

class PathBuilder(rootFolderPath: String) {

  def getPropertiesFilePath: String = s"$getTargetFolderPath/$propertyFileName"

  def getWorkflowFilePath(workflowFileName: String): String = s"$getWorkflowFolderPath/$workflowFileName"

  def getWorkflowFolderPath: String = s"$getTargetFolderPath/$workflowFolderName"

  def getTargetFolderPath: String = if (rootFolderPath.endsWith("/")) rootFolderPath.dropRight(1) else rootFolderPath

  def getCoordinatorFilePath(coordinatorFileName: String): String = s"$getCoordinatorFolderPath/$coordinatorFileName"

  def getCoordinatorFolderPath: String = s"$getTargetFolderPath/$coordinatorFolderName"

  def getBundleFilePath(bundleFileName: String): String = s"$getBundleFolderPath/$bundleFileName"

  def getBundleFolderPath: String = s"$getTargetFolderPath/$bundleFolderName"

  def getScriptFilePath(scriptName: String): String = s"$getScriptsFolderPath/$scriptName"

  def getScriptsFolderPath: String = s"$getTargetFolderPath/$scriptFolderName"
}