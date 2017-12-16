package com.github.izhangzhihao.soozie.jobs

import com.github.izhangzhihao.soozie.dsl.{ConfigBuilder, ConfigurationList, Job}
import com.github.izhangzhihao.soozie.utils.WriterUtils
import oozie.shell._

import scalaxb.DataRecord

case class ShellScript(script: String) extends AnyVal

case class ShellScriptDescriptor(name: String, script: String)

case class ShellJob[T](jobName: String, record: DataRecord[T], descriptor: Option[ShellScriptDescriptor]) extends Job[T]

object ShellJob {
  def apply(jobName: String,
            exec: Either[String, ShellScript],
            jobTracker: Option[String] = None,
            nameNode: Option[String] = None,
            prepare: Option[PREPARE] = None,
            jobXml: Seq[String] = Nil,
            configuration: ConfigurationList = Nil,
            arguments: Seq[String] = Nil,
            environmentVariables: Seq[String] = Nil,
            file: Seq[String] = Nil,
            archive: Seq[String] = Nil,
            captureOutput: Boolean = false): ShellJob[ACTION] = v0_3(
    jobName,
    exec,
    jobTracker,
    nameNode,
    prepare,
    jobXml,
    configuration,
    arguments,
    environmentVariables,
    file,
    archive,
    captureOutput
  )

  def v0_3(jobName: String,
           exec: Either[String, ShellScript],
           jobTracker: Option[String] = None,
           nameNode: Option[String] = None,
           prepare: Option[PREPARE] = None,
           jobXml: Seq[String] = Nil,
           configuration: ConfigurationList = Nil,
           arguments: Seq[String] = Nil,
           environmentVariables: Seq[String] = Nil,
           file: Seq[String] = Nil,
           archive: Seq[String] = Nil,
           captureOutput: Boolean = false) = {

    val configBuilderImpl = new ConfigBuilder[CONFIGURATION, Property] {
      override def build(property: Seq[Property]): CONFIGURATION = CONFIGURATION(property)

      override def buildProperty(name: String, value: String, description: Option[String]): Property = Property(name, value, description)
    }

    val _jobName = jobName

    exec match {
      case Left(oozieExecString) =>
        new ShellJob[ACTION](
          jobName = _jobName,
          record = ???,
          //            DataRecord(None, Some("shell"),
          //            ACTION(
          //            jobu45tracker = jobTracker,
          //            nameu45node = nameNode,
          //            prepare = prepare,
          //            jobu45xml = jobXml,
          //            configuration = configBuilderImpl(configuration),
          //            exec = oozieExecString,
          //            argument = arguments,
          //            envu45var = environmentVariables,
          //            file = file,
          //            archive = archive,
          //            captureu45output = if (captureOutput) Some(FLAG()) else None,
          //            xmlns = "uri:oozie:shell-action")
          //          ),
          descriptor = None
        )
      case Right(shellScript) =>
        import com.github.izhangzhihao.soozie.SoozieConfig._
        import com.github.izhangzhihao.soozie.utils.PropertyImplicits._

        new ShellJob[ACTION](
          jobName = _jobName,
          record = ???,
          //            DataRecord(None, Some("shell"), ACTION(
          //            jobu45tracker = jobTracker,
          //            nameu45node = nameNode,
          //            prepare = prepare,
          //            jobu45xml = jobXml,
          //            configuration = configBuilderImpl(configuration),
          //            exec = s"$jobName.$scriptExtension",
          //            argument = arguments,
          //            envu45var = environmentVariables,
          //            file = file ++ Seq(s"${WriterUtils.buildPathPropertyName(jobName).toParameter}#$jobName.$scriptExtension"),
          //            archive = archive,
          //            captureu45output = if (captureOutput) Some(FLAG()) else None,
          //            xmlns = "uri:oozie:shell-action")
          //          ),
          descriptor = Some(ShellScriptDescriptor(jobName, shellScript.script))
        )
    }
  }
}
