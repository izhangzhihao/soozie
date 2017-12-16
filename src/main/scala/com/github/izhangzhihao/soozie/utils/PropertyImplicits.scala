package com.github.izhangzhihao.soozie.utils

import java.io.StringWriter
import java.util.Properties

object PropertyImplicits {
  implicit class StringWrapper(val string: String) extends AnyVal {
    def toParameter: String = s"$${$string}"
  }

  implicit class MapWrapper(val map: Map[String, String]) extends AnyVal {
    def getAsParameter(key: String): String = {
      map.get(key) match {
        case Some(_) => s"$${$key}"
        case None => throw new NoSuchElementException(s"The key($key) does not exist in the map($map)")
      }
    }

    def toProperties: Properties = {
      import scala.collection.JavaConversions._
      val properties = new java.util.Properties
      properties.putAll(map)
      properties

    }
  }

  implicit class PropertyWrapper(val properties: Properties) extends AnyVal {
    def toWritableString = {
      val stringWriter = new StringWriter()
      properties.store(stringWriter, "")

      stringWriter.getBuffer.toString
    }
  }
}
