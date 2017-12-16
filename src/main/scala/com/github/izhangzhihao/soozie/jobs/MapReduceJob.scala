package com.github.izhangzhihao.soozie.jobs

import com.github.izhangzhihao.soozie.dsl._
import oozie.workflow._

import scalaxb.DataRecord

object MapReduceJob {
  def apply(jobName: String,
            nameNode: Option[String] = None,
            jobTracker: Option[String] = None,
            prepare: Option[PREPARE] = None,
            configuration: ConfigurationList = Nil,
            mapReduceOption: Option[DataRecord[MAPu45REDUCEOption]] = None,
            jobXml: Seq[String] = Nil,
            configClass: Option[String] = None,
            file: Seq[String] = Nil,
            archive: Seq[String] = Nil): Job[MAPu45REDUCE] = {
    v0_5(
      jobName,
      nameNode,
      jobTracker,
      prepare,
      configuration,
      mapReduceOption,
      jobXml,
      configClass,
      file,
      archive
    )
  }

  def v0_5(jobName: String,
           nameNode: Option[String] = None,
           jobTracker: Option[String] = None,
           prepare: Option[PREPARE] = None,
           configuration: ConfigurationList = Nil,
           mapReduceOption: Option[DataRecord[MAPu45REDUCEOption]] = None,
           jobXml: Seq[String] = Nil,
           configClass: Option[String] = None,
           file: Seq[String] = Nil,
           archive: Seq[String] = Nil): Job[MAPu45REDUCE] = {

    val _jobName = jobName

    val configBuilderImpl = new ConfigBuilder[CONFIGURATION, Property2] {
      override def build(property: Seq[Property2]): CONFIGURATION = CONFIGURATION(property)

      override def buildProperty(name: String, value: String, description: Option[String]): Property2 = Property2(name, value, description)
    }

    new Job[MAPu45REDUCE] {
      override val jobName = _jobName
      override val record = ???
//        DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
//        jobu45tracker = jobTracker,
//        nameu45node = nameNode,
//        prepare = prepare,
//        configuration = configBuilderImpl(configuration),
//        mapu45reduceoption = mapReduceOption,
//        jobu45xml = jobXml,
//        configu45class = configClass,
//        file = file,
//        archive = archive
//      ))
    }
  }
}
