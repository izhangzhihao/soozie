package com.github.izhangzhihao.soozie.jobs

import com.github.izhangzhihao.soozie.dsl.{ConfigBuilder, ConfigurationList, Job}
import oozie.hive_0_5._

import scalaxb._
import oozie._

object HiveJob {
  def apply(jobName: String,
            fileName: String,
            nameNode: Option[String] = None,
            jobTracker: Option[String] = None,
            configuration: ConfigurationList = Nil,
            parameters: List[String] = Nil,
            prepare: Option[PREPARE] = None,
            jobXml: Option[Seq[String]] = None,
            otherFiles: Option[Seq[String]] = None,
            file: Seq[String] = Nil,
            argument: Seq[String] = Nil,
            archive: Seq[String] = Nil): Job[ACTION] = v0_5(
    jobName,
    fileName,
    nameNode,
    jobTracker,
    configuration,
    parameters,
    prepare,
    jobXml,
    file,
    argument,
    archive
  )

  def v0_5(jobName: String,
           fileName: String,
           nameNode: Option[String] = None,
           jobTracker: Option[String] = None,
           configuration: ConfigurationList = Nil,
           parameters: List[String] = Nil,
           prepare: Option[PREPARE] = None,
           jobXml: Option[Seq[String]] = None,
           file: Seq[String] = Nil,
           argument: Seq[String] = Nil,
           archive: Seq[String] = Nil): Job[ACTION] = {

    val configBuilderImpl = new ConfigBuilder[CONFIGURATION, Property] {
      override def build(property: Seq[Property]): CONFIGURATION = CONFIGURATION(property)

      override def buildProperty(name: String, value: String, description: Option[String]): Property =
        Property(name, value, description)
    }

    new Job[ACTION] {
      val dotIndex = fileName.indexOf(".")
      val cleanName = {
        if (dotIndex > 0)
          fileName.substring(0, dotIndex)
        else
          fileName
      }
      override val jobName = s"hive_$cleanName"
      override val record =
        DataRecord(None, Some("hive"), ACTION(
          script = fileName,
          jobu45tracker = jobTracker,
          nameu45node = nameNode,
          jobu45xml = jobXml match {
            case Some(xml) => xml
            case _ => Seq[String]()
          },
          configuration = configBuilderImpl(configuration),
          param = parameters,
          prepare = prepare,
          file = file,
          argument = argument,
          archive = archive,
          xmlns = "uri:oozie:hive-action:0.5"
        ))
    }
  }
}
