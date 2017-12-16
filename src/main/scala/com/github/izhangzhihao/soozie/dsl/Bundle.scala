package com.github.izhangzhihao.soozie.dsl

import oozie.bundle._
import org.joda.time.DateTime

trait Bundle[B, C, W] extends WritableApplication {
  type Coordinator

  val kickoffTime: Either[DateTime, String]
  val parameters: ParameterList
  val coordinators: List[CoordinatorDescriptor[C, W]]

  def buildCoordinator(name: String, configuration: ConfigurationList, path: String): Coordinator

  def buildBundle(name: String, kickoffTime: String, coordinatorDescriptors: Seq[Coordinator]): B
}

case class CoordinatorDescriptor[C, W](name: String,
                                       coordinator: Coordinator[C, W],
                                       configuration: ConfigurationList = Nil,
                                       path: Option[String] = None)

object Bundle {
  def apply[C, W, A](name: String,
                     parameters: ParameterList = Nil,
                     kickoffTime: Either[DateTime, String],
                     coordinators: List[CoordinatorDescriptor[C, W]]) = v0_2(name, parameters, kickoffTime, coordinators)

  def v0_2[C, W, A](name: String,
                    parameters: ParameterList = Nil,
                    kickoffTime: Either[DateTime, String],
                    coordinators: List[CoordinatorDescriptor[C, W]]) = {

    val parameterBuilderImpl = new ParameterBuilder[PARAMETERS, Property] {
      override def build(property: Seq[Property]): PARAMETERS = PARAMETERS(property)

      override def buildProperty(name: String, value: Option[String], description: Option[String]): Property = Property(name, value, description)
    }

    val configBuilderImpl = new ConfigBuilder[CONFIGURATION, Property2] {
      override def build(property: Seq[Property2]): CONFIGURATION = CONFIGURATION(property)

      override def buildProperty(name: String, value: String, description: Option[String]): Property2 = Property2(name, value, description)
    }

    val _name = name
    val _parameters = parameters
    val _kickoffTime = kickoffTime
    val _coordinators = coordinators

    new Bundle[BUNDLEu45APP, C, W] {
      type Coordinator = COORDINATOR

      override val name: String = _name
      override val parameters: ParameterList = _parameters
      override val kickoffTime: Either[DateTime, String] = _kickoffTime
      override val coordinators: List[CoordinatorDescriptor[C, W]] = _coordinators

      override val namespace: String = "bundle"
      override val elementLabel: String = "bundle-app"
      override val scope: String = "uri:oozie:bundle"

      def buildCoordinator(name: String, configuration: ConfigurationList, path: String): Coordinator = {
        COORDINATOR(
          appu45path = path,
          //          name = name,
          configuration = configBuilderImpl(configuration)
        )
      }

      override def buildBundle(name: String,
                               kickoffTime: String,
                               coordinators: Seq[Coordinator]): BUNDLEu45APP = {
        BUNDLEu45APP(
          //          name = name,
          parameters = parameterBuilderImpl(parameters),
          controls = Some(CONTROLS(Some(CONTROLSSequence1(Some(kickoffTime))))),
          coordinator = coordinators
        )
      }
    }
  }
}