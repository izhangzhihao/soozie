package com.github.izhangzhihao.soozie.jobs

import com.github.izhangzhihao.soozie.dsl._
import oozie.workflow.{ JAVA, PREPARE, FLAG, CONFIGURATION, Property2 }
import oozie._

import scalaxb.DataRecord

object JavaJob {
  def apply(jobName: String,
            mainClass: String,
            jobTracker: Option[String] = None,
            nameNode: Option[String] = None,
            prepare: Option[PREPARE] = None,
            configuration: ConfigurationList = Nil,
            jvmOps: Option[String] = None,
            args: List[String] = Nil,
            jobXml: Seq[String] = Nil,
            file: Seq[String] = Nil,
            archive: Seq[String] = Nil,
            captureOutput: Boolean = false): Job[JAVA] = {
    v0_5(
      jobName,
      mainClass,
      jobTracker,
      nameNode,
      prepare,
      configuration,
      jvmOps,
      args,
      jobXml,
      file,
      archive,
      captureOutput
    )
  }

  def v0_5(jobName: String,
           mainClass: String,
           jobTracker: Option[String] = None,
           nameNode: Option[String] = None,
           prepare: Option[PREPARE] = None,
           configuration: ConfigurationList = Nil,
           jvmOps: Option[String] = None,
           args: List[String] = Nil,
           jobXml: Seq[String] = Nil,
           file: Seq[String] = Nil,
           archive: Seq[String] = Nil,
           captureOutput: Boolean = false): Job[JAVA] = {

    val configBuilderImpl = new ConfigBuilder[CONFIGURATION, Property2] {
      override def build(property: Seq[Property2]): CONFIGURATION = CONFIGURATION(property)

      override def buildProperty(name: String, value: String, description: Option[String]): Property2 = Property2(name, value, description)
    }

    val _jobName = jobName

    new Job[JAVA] {
      override val jobName = _jobName
      override val record = ???
//        DataRecord(None, Some("java"), JAVA(
//        jobu45tracker = jobTracker,
//        nameu45node = nameNode,
//        mainu45class = mainClass,
//        prepare = prepare,
//        configuration = configBuilderImpl(configuration),
//        javaoption = Seq(DataRecord(jvmOps.getOrElse(""))),
//        arg = args,
//        jobu45xml = jobXml,
//        file = file,
//        archive = archive,
//        captureu45output = if (captureOutput) Some(FLAG()) else None)
//      )
    }
  }
}