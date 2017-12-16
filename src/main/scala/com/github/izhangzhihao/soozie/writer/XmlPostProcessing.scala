package com.github.izhangzhihao.soozie.writer

case class XmlPostProcessing(substitutions: Map[String, String])

object XmlPostProcessing {
  val Default = XmlPostProcessing(
    substitutions = Map(
      "&quot;" -> "\"")
  )
}