package com.github.izhangzhihao.soozie.dsl

import scalaxb._

sealed trait Work {
  def dependsOn(dep1: Dependency, deps: Dependency*): Node = Node(this, List(dep1) ++ deps)

  def dependsOn(deps: Seq[Dependency]): Node = Node(this, deps.toList)

  def dependsOn(sugarNode: SugarNode): SugarNode = SugarNode(this, sugarNode.dependency, Some(sugarNode))
}

case object End extends Work

trait Job[T] extends Work {
  val jobName: String
  val record: DataRecord[T]
}

case class Kill(name: String) extends Work

sealed trait Dependency

case class ForkDependency(name: String) extends Dependency

case class JoinDependency(name: String) extends Dependency

case class Node(work: Work, dependencies: List[_ <: Dependency]) extends Dependency {

  def doIf(predicate: String) = {
    //make sure predicate string is in ${foo} format
    val Pattern =
      """[${].*[}]""".r
    val formattedPredicate = predicate match {
      case Pattern() => predicate
      case _ => "${" + predicate + "}"
    }
    val decision = Decision(formattedPredicate -> Predicates.BooleanProperty(formattedPredicate)) dependsOn dependencies
    SugarNode(work, decision option formattedPredicate)
  }

  def error = ErrorTo(this) //used to set a custom error-to on a node
}

case object Start extends Dependency

case class OneOf(dep1: Dependency, deps: Dependency*) extends Dependency

case class SugarNode(work: Work, dependency: DecisionDependency, previousSugarNode: Option[SugarNode] = None)

object Optional {
  def toNode(sugarNode: SugarNode): Node = sugarNode.previousSugarNode match {
    case Some(previous) => Node(sugarNode.work, List(toNode(previous)))
    case _ => Node(sugarNode.work, List(sugarNode.dependency))
  }

  def apply(sugarNode: SugarNode) = OneOf(sugarNode.dependency.parent default, toNode(sugarNode))
}

sealed trait Predicate

object Predicates {

  case class BooleanProperty(property: String) extends Predicate {
    lazy val formattedProperty = property match {
      case BooleanPropertyRegex(_) => property
      case _ => """${%s}""" format property
    }
    val BooleanPropertyRegex = """\$\{(.*)\}""" r
  }

  case object AlwaysTrue extends Predicate

}

case class Decision(predicates: List[(String, Predicate)]) {
  def dependsOn(dep1: Dependency, deps: Dependency*): DecisionNode = DecisionNode(this, Set(dep1) ++ deps)

  def dependsOn(deps: Seq[Dependency]): DecisionNode = DecisionNode(this, deps.toSet)
}

object Decision {
  def apply(pair1: (String, Predicate), pairs: (String, Predicate)*): Decision = Decision(pair1 :: pairs.toList)
}

case class DecisionDependency(parent: DecisionNode, option: Option[String]) extends Dependency

case class DecisionNode(decision: Decision, dependencies: Set[_ <: Dependency]) extends Dependency {
  val default: Dependency = DecisionDependency(this, None)
  val option: String => DecisionDependency = name => DecisionDependency(this, Some(name))
}

case class DoIf(predicate: String, deps: Dependency*) extends Dependency

case class ErrorTo(node: Node) extends Dependency

trait WritableApplication {
  val name: String

  val scope: String
  val namespace: String
  val elementLabel: String
}

trait ConfigBuilder[Configuration, Property] {
  def build(property: Seq[Property]): Configuration

  def buildProperty(name: String, value: String, description: Option[String] = None): Property

  def apply(config: ConfigurationList): Option[Configuration] = {
    if (config.nonEmpty)
      Some(build(config.map { case (propName, propValue) => buildProperty(propName, propValue) }))
    else
      None
  }
}

trait ParameterBuilder[Parameter, Property] {
  def build(property: Seq[Property]): Parameter

  def buildProperty(name: String, value: Option[String] = None, description: Option[String] = None): Property

  def apply(config: ParameterList): Option[Parameter] = {
    if (config.nonEmpty)
      Some(build(config.map { case (propName, propValue) => buildProperty(propName, propValue) }))
    else
      None
  }
}

trait ActionBuilder[ActionOption] {
  type Predicate = String
  type Route = String

  def buildAction(name: String,
                  actionOption: Job[ActionOption],
                  okTo: String,
                  errorTo: String): DataRecord[ActionOption]

  def buildJoin(name: String, okTo: String): DataRecord[ActionOption]

  def buildFork(name: String, afterNames: List[String]): DataRecord[ActionOption]

  def buildDecision(name: String, defaultName: String, cases: List[(Predicate, Route)]): DataRecord[ActionOption]

  def buildKill(name: String): DataRecord[ActionOption]
}

trait Workflow[W] extends Work with WritableApplication {
  type ActionOption

  val end: Node
  val actionBuilder: ActionBuilder[ActionOption]

  def buildWorkflow(start: String, end: String, actions: Seq[DataRecord[ActionOption]]): W
}

object Workflow {
  def apply(name: String,
            end: Node,
            parameters: ParameterList = Nil,
            global: Option[oozie.workflow.GLOBAL] = None,
            credentials: Option[oozie.workflow.CREDENTIALS] = None,
            any: Option[oozie.sla.SLAu45INFO] = None): Workflow[oozie.workflow.WORKFLOWu45APP] =
    v0_5(name, end, parameters, global, credentials, any)

  def v0_5(name: String,
           end: Node,
           parameters: ParameterList = Nil,
           global: Option[oozie.workflow.GLOBAL] = None,
           credentials: Option[oozie.workflow.CREDENTIALS] = None,
           any: Option[oozie.sla.SLAu45INFO] = None): Workflow[oozie.workflow.WORKFLOWu45APP] = {

    import oozie.workflow._

    val actionBuilderImpl = new ActionBuilder[WORKFLOWu45APPOption] {
      def buildAction(name: String,
                      actionOption: Job[WORKFLOWu45APPOption],
                      okTo: String,
                      errorTo: String): DataRecord[WORKFLOWu45APPOption] = ???

      //        DataRecord(None, Some("action"), ACTION(
      //          name = name,
      //          actionoption = actionOption.record,
      //          ok = ACTION_TRANSITION(okTo),
      //          error = ACTION_TRANSITION(errorTo)
      //        ))

      def buildJoin(name: String, okTo: String): DataRecord[WORKFLOWu45APPOption] = ???

      //        DataRecord(None, Some("join"), JOIN(name = name, to = okTo))

      def buildFork(name: String, afterNames: List[String]): DataRecord[WORKFLOWu45APPOption] = ???

      //        DataRecord(None, Some("fork"),
      //                    FORK(
      //                      path = afterNames.map(FORK_TRANSITION),
      //                      name = name
      //                    )
      //        )

      def buildDecision(name: String,
                        defaultName: String,
                        cases: List[(Predicate, Route)]): DataRecord[WORKFLOWu45APPOption] = ???

      //        DataRecord(None, Some("decision"), DECISION(
      //          name = name,
      //          switch = SWITCH(
      //            switchsequence1 = SWITCHSequence1(
      //              caseValue = cases.map { case (predicate, route) => CASE(predicate, route) },
      //              default = DEFAULT(defaultName)))))

      def buildKill(workflowName: String): DataRecord[WORKFLOWu45APPOption] = ???

      //        DataRecord(None, Some("kill"), KILL(
      //          message = workflowName + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
      //          name = "kill"))
    }

    val parameterBuilderImpl = new ParameterBuilder[PARAMETERS, Property] {
      override def build(property: Seq[Property]): PARAMETERS = PARAMETERS(property)

      override def buildProperty(name: String, value: Option[String], description: Option[String]): Property = Property(name, value, description)
    }

    val _name = name
    val _end = end

    new com.github.izhangzhihao.soozie.dsl.Workflow[WORKFLOWu45APP] {
      type ActionOption = WORKFLOWu45APPOption

      override val name = _name
      override val end = _end
      override val scope = "uri:oozie:workflow"
      override val namespace = "workflow"
      override val elementLabel = "workflow-app"

      override val actionBuilder = actionBuilderImpl

      override def buildWorkflow(start: String, end: String, actions: Seq[DataRecord[ActionOption]]) = ???

      //      {
      //        WORKFLOWu45APP(name = name,
      //          start = START(start),
      //          end = END(end),
      //          parameters = parameterBuilderImpl(parameters),
      //          global = global,
      //          credentials = credentials,
      //          workflowu45appoption = actions,
      //          any = any.map(DataRecord(None, Some("sla"), _))
      //        )
      //      }
    }
  }
}
