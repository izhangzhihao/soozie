package com.github.izhangzhihao.soozie.jobs

import com.github.izhangzhihao.soozie.dsl.{ConfigBuilder, ConfigurationList, Job}
import oozie.sqoop_0_4._

import scalaxb._
import oozie._

object SqoopJob {

  def apply(jobName: String,
            nameNode: Option[String] = None,
            jobTracker: Option[String] = None,
            configuration: ConfigurationList = Nil,
            prepare: Option[PREPARE] = None,
            jobXml: Option[Seq[String]] = None,
            file: Seq[String] = Nil,
            actionoption: Seq[DataRecord[String]] = Nil,
            archive: Seq[String] = Nil): Job[ACTION] =
    v0_4(jobName, nameNode, jobTracker, configuration, prepare, jobXml, file, actionoption, archive)


  def v0_4(jobname: String,
           nameNode: Option[String] = None,
           jobTracker: Option[String] = None,
           configuration: ConfigurationList = Nil,
           prepare: Option[PREPARE] = None,
           jobXml: Option[Seq[String]] = None,
           file: Seq[String] = Nil,
           actionoption: Seq[scalaxb.DataRecord[String]] = Nil,
           archive: Seq[String] = Nil): Job[ACTION] = {

    val configBuilderImpl = new ConfigBuilder[CONFIGURATION, Property] {
      override def build(property: Seq[Property]): CONFIGURATION = CONFIGURATION(property)

      override def buildProperty(name: String, value: String, description: Option[String]): Property =
        Property(name, value, description)
    }
    new Job[ACTION] {
      override val jobName = jobname
      override val record =
        DataRecord(None, Some("sqoop"), ACTION(
          jobu45tracker = jobTracker,
          nameu45node = nameNode,
          prepare = prepare,
          jobu45xml = jobXml match {
            case Some(xml) => xml
            case _ => Seq[String]()
          },
          configuration = configBuilderImpl(configuration),
          actionoption = actionoption,
          file = file,
          archive = archive,
          xmlns = "uri:oozie:sqoop-action:0.4"
        ))
    }
  }
}
