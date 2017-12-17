package com.github.izhangzhihao.soozie.jobs

import com.github.izhangzhihao.soozie.dsl.Job
import oozie.email._
import oozie.XMLProtocol._
import scalaxb.DataRecord

object EmailJob {
  def apply(jobName: String,
            to: String,
            subject: String,
            body: String,
            cc: Option[String] = None,
            bcc: Option[String] = None,
            contentType: Option[String] = None,
            attachment: Option[String] = None): Job[ACTION] = v0_1(
    jobName,
    to,
    subject,
    body,
    cc,
    bcc,
    contentType,
    attachment
  )

  def v0_1(jobName: String,
           to: String,
           subject: String,
           body: String,
           cc: Option[String] = None,
           bcc: Option[String] = None,
           contentType: Option[String] = None,
           attachment: Option[String] = None): Job[ACTION] = {

    val _jobName = jobName

    new Job[ACTION] {
      override val jobName = _jobName
      override val record: DataRecord[ACTION] =
        DataRecord(None, Some("email"), ACTION(
          to = to,
          cc = cc,
          bcc = bcc,
          subject = subject,
          body = body,
          content_type = contentType,
          attachment = attachment,
          xmlns = "uri:oozie:email-action:0.2")
        )
    }
  }
}