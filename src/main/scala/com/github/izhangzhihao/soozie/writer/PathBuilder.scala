package com.github.izhangzhihao.soozie.writer

import com.github.izhangzhihao.soozie.ScoozieConfig._

class PathBuilder(rootFolderPath: String) {

  def getTargetFolderPath: String = if(rootFolderPath.endsWith("/")) rootFolderPath.dropRight(1) else rootFolderPath

  def getWorkflowFolderPath: String = s"$getTargetFolderPath/$workflowFolderName"

  def getCoordinatorFolderPath: String = s"$getTargetFolderPath/$coordinatorFolderName"

  def getBundleFolderPath: String = s"$getTargetFolderPath/$bundleFolderName"

  def getPropertiesFilePath: String = s"$getTargetFolderPath/$propertyFileName"

  def getWorkflowFilePath(workflowFileName: String): String = s"$getWorkflowFolderPath/$workflowFileName"

  def getCoordinatorFilePath(coordinatorFileName: String): String = s"$getCoordinatorFolderPath/$coordinatorFileName"

  def getBundleFilePath(bundleFileName: String): String = s"$getBundleFolderPath/$bundleFileName"

  def getScriptsFolderPath: String = s"$getTargetFolderPath/$scriptFolderName"

  def getScriptFilePath(scriptName: String): String = s"$getScriptsFolderPath/$scriptName"
}