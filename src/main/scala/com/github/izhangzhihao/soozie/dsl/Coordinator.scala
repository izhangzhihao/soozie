package com.github.izhangzhihao.soozie.dsl

import com.github.izhangzhihao.soozie.utils.CronConverter.{ CronSubExpr, DayOfMonth, DayOfWeek, Hour, Minute, Month }
import oozie.coordinator._
import org.joda.time.{ DateTime, DateTimeZone }

trait Coordinator[C, W] extends WritableApplication {
  val start: DateTime
  val end: DateTime
  val timezone: DateTimeZone
  val frequency: Frequency
  val configuration: ConfigurationList
  val workflow: Workflow[W]
  val workflowPath: Option[String]

  def buildCoordinator(start: String,
                       end: String,
                       frequency: String,
                       timezone: String,
                       workflowPath: String): C
}

object Coordinator {
  def apply[W](name: String,
               workflow: Workflow[W],
               start: DateTime,
               end: DateTime,
               timezone: DateTimeZone,
               frequency: Frequency,
               configuration: ConfigurationList = Nil,
               workflowPath: Option[String] = None,
               parameters: ParameterList = Nil,
               controls: Option[CONTROLS] = None,
               datasets: Option[DATASETS] = None,
               inputEvents: Option[INPUTEVENTS] = None,
               outputEvents: Option[OUTPUTEVENTS] = None) =
    v0_4(
      name,
      workflow,
      start,
      end,
      timezone,
      frequency,
      configuration,
      workflowPath,
      parameters,
      controls,
      datasets,
      inputEvents,
      outputEvents)

  def v0_4[W](name: String,
              workflow: Workflow[W],
              start: DateTime,
              end: DateTime,
              timezone: DateTimeZone,
              frequency: Frequency,
              configuration: ConfigurationList = Nil,
              workflowPath: Option[String] = None,
              parameters: ParameterList = Nil,
              controls: Option[CONTROLS] = None,
              datasets: Option[DATASETS] = None,
              inputEvents: Option[INPUTEVENTS] = None,
              outputEvents: Option[OUTPUTEVENTS] = None): Coordinator[COORDINATORu45APP, W] = {

    val _name = name
    val _start = start
    val _end = end
    val _timezone = timezone
    val _frequency = frequency
    val _configuration = configuration
    val _workflowPath = workflowPath
    val _workflow = workflow

    val configBuilderImpl = new ConfigBuilder[CONFIGURATION, Property2] {
      override def build(property: Seq[Property2]): CONFIGURATION = CONFIGURATION(property)

      override def buildProperty(name: String, value: String, description: Option[String]): Property2 = Property2(name, value, description)
    }

    val parameterBuilderImpl = new ParameterBuilder[PARAMETERS, Property] {
      override def build(property: Seq[Property]): PARAMETERS = PARAMETERS(property)

      override def buildProperty(name: String, value: Option[String], description: Option[String]): Property = Property(name, value, description)
    }

    new Coordinator[COORDINATORu45APP, W] {
      override val name: String = _name
      override val start: DateTime = _start
      override val end: DateTime = _end
      override val timezone: DateTimeZone = _timezone
      override val frequency: Frequency = _frequency
      override val configuration: ConfigurationList = _configuration
      override val workflow: Workflow[W] = _workflow
      override val workflowPath: Option[String] = _workflowPath

      override val namespace: String = "coordinator"
      override val elementLabel: String = "coordinator-app"
      override val scope: String = "uri:oozie:coordinator"

      override def buildCoordinator(start: String,
                                    end: String,
                                    frequency: String,
                                    timezone: String,
                                    workflowPath: String): COORDINATORu45APP = {
        COORDINATORu45APP(
          action = ACTION(WORKFLOW(
            appu45path = workflowPath,
            configuration = configBuilderImpl(configuration)
          )),
//          frequency = frequency,
//          start = start,
//          end = end,
//          timezone = timezone,
//          name = name,
          parameters = parameterBuilderImpl(parameters),
          controls = controls,
          datasets = datasets,
          inputu45events = inputEvents,
          outputu45events = outputEvents
        )
      }
    }
  }
}

sealed trait Frequency

case class Minutes(n: Int) extends Frequency {
  override def toString = s"$${coord:minutes($n)}"
}

case class Hours(n: Int) extends Frequency {
  override def toString = s"$${coord:hours($n)}"
}

case class Days(n: Int) extends Frequency {
  override def toString = s"$${coord:days($n)}"
}

case class Months(n: Int) extends Frequency {
  override def toString = s"$${coord:months($n)}"
}

case class Cron(minutes: CronSubExpr[Minute] = Set[Int](),
                hours: CronSubExpr[Hour] = Set[Int](),
                dayOfMonth: CronSubExpr[DayOfMonth] = Set[Int](),
                month: CronSubExpr[Month] = Set[Int](),
                dayOfWeek: CronSubExpr[DayOfWeek] = Set[Int]()) extends Frequency {

  override def toString = productIterator.mkString(" ")
}

