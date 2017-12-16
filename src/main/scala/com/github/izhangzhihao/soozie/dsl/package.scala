package com.github.izhangzhihao.soozie

package object dsl {
  type Name = String
  type Value = String
  type ConfigurationList = List[(Name, Value)]
  type ParameterList = List[(Name, Option[Value])]
}