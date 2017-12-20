package com.github.izhangzhihao.soozie.conversion

import com.github.izhangzhihao.soozie.dsl._

case class RefSet[A <: AnyRef](vals: Seq[A]) extends Set[A] {

  def iterator: Iterator[A] = vals.iterator

  def ++(elems: RefSet[A]): RefSet[A] = (this /: elems) (_ + _)

  def --(elems: RefSet[A]): RefSet[A] = (this /: elems) (_ - _)

  def -(elem: A): RefSet[A] = {
    if (!(this contains elem))
      this
    else
      RefSet(vals filter (_ ne elem))
  }

  def map[B <: AnyRef](f: (A) => B): RefSet[B] = {
    (RefSet[B]() /: vals) ((e1: RefSet[B], e2: A) => e1 + f(e2))
  }

  def +(elem: A): RefSet[A] = {
    if (this contains elem)
      this
    else
      RefSet(vals :+ elem)
  }

  override def contains(elem: A): Boolean = vals exists (e => e eq elem)

  override def equals(that: Any): Boolean = {
    that match {
      case RefSet(otherVals) =>
        this.vals.toSet.equals(otherVals.toSet)
      case _ => false
    }
  }

}

object RefSet {
  def apply[A <: AnyRef](): RefSet[A] = {
    RefSet(Seq.empty)
  }

  def apply[A <: AnyRef](elem: A, elems: A*): RefSet[A] = {
    RefSet(elem +: elems)
  }
}

/*
 * Map that compares keys by reference
 */
case class RefMap[A <: AnyRef, B](vals: Map[RefWrap[A], B]) extends Map[RefWrap[A], B] {

  def +[B1 >: B](kv: => (A, B1)): RefMap[A, B1] = {
    val newKv = RefWrap(kv._1) -> kv._2
    RefMap(vals + newKv)
  }

  def ++(rmap: RefMap[A, B]) = {
    (this /: rmap) (_ + _)
  }

  def +[B1 >: B](kv: (RefWrap[A], B1)) = {
    RefMap(vals + kv)
  }

  def -(key: RefWrap[A]) = {
    RefMap(vals - key)
  }

  def get(key: RefWrap[A]): Option[B] = {
    vals get key
  }

  def get(key: => A): Option[B] = {
    vals get RefWrap(key)
  }

  def iterator: Iterator[(RefWrap[A], B)] = vals.iterator
}

case class RefWrap[T <: AnyRef](value: T) {
  override def equals(other: Any) = other match {
    case ref: RefWrap[_] => ref.value eq value
    case _ => false
  }
}

object Flatten {
  def apply[T](workflow: Workflow[T]): RefMap[Dependency, GraphNode] = {

    var accum: RefMap[Dependency, GraphNode] = RefMap[Dependency, GraphNode](Map.empty)

    def flatten0(currentDep: Dependency, after: Set[GraphNode], inDecision: Boolean = false) {
      accum get currentDep match {
        //check if we've already processed the dependency. If so, just update after to include where we came from
        case Some(alreadyThere) =>
          currentDep match {
            case DecisionNode(decision, dependencies) =>
              alreadyThere.decisionAfter ++= RefSet(after.toSeq)
            case _ =>
              after foreach (alreadyThere.after += _)
          }
        case _ =>
          currentDep match {

            case Node(End, deps) =>
              val endNode = GraphNode("end", WorkflowEnd)
              deps foreach (flatten0(_, Set(endNode)))

            case Node(job: Job[_], deps) =>
              val newNode = GraphNode(
                job.jobName,
                WorkflowJob(job),
                before = RefSet(),
                after = RefSet(after.toSeq))
              accum += currentDep -> newNode
              deps foreach (flatten0(_, Set(newNode), inDecision))

            case Node(wf: Workflow[_], deps) =>
              val wfAccum = Flatten(wf)
              accum ++= wfAccum
              //special case for the last nodes in the workflow: let them know what's after them
              val lastNodes = wfAccum.values filter isPotentialEndNode
              val lastNodeHeadOption = lastNodes headOption

              lastNodeHeadOption foreach { lastNode =>
                //remove previous reference from map
                wfAccum.find(elem => elem._2 == lastNode) match {
                  case Some(pair) => accum -= pair._1
                  case _ =>
                }
                //update reference to last nodes
                accum += currentDep -> lastNode
              }
              lastNodes foreach { lastNode =>
                if (!inDecision && lastNodes.size == 1)
                  lastNode.after = RefSet(after.toSeq)
                else {
                  //remove "end" from lastNode's decisionAfter
                  val end = lastNode.decisionAfter filter (_.workflowOption == WorkflowEnd) headOption
                  val decisionRoutes = end match {
                    case Some(node) => node.decisionRoutes
                    case _ => Set.empty
                  }
                  end foreach (lastNode.decisionAfter -= _)
                  //add in new decisionAfter
                  lastNode.decisionAfter ++= RefSet(after.toSeq)
                  after foreach (after => {
                    after.decisionRoutes = after.decisionRoutes ++ decisionRoutes
                  })
                  lastNode.after = RefSet()
                }
              }
              //recur on the "starting" nodes of this subwf
              val newAfter = (wfAccum.values filter (node => isStartNode(node))).toSet
              deps foreach { newCurrent =>
                flatten0(newCurrent, newAfter, inDecision)
              }
            //check if we're dealing with a Decision
            case OneOf(dep1, deps@_*) =>
              //recur on everything this is dependent on
              (List(dep1) ++ deps) foreach (currDep => {
                accum get currDep match {
                  case Some(alreadyThere) =>
                    alreadyThere.decisionAfter ++= RefSet(after.toSeq)
                  case _ =>
                    currDep match {
                      case Node(job: Job[_], deps) =>
                        val newNode = GraphNode(
                          job.jobName,
                          WorkflowJob(job),
                          before = RefSet(),
                          after = RefSet(),
                          decisionAfter = RefSet(after.toSeq))
                        accum += currDep -> newNode
                        deps foreach (flatten0(_, Set(newNode), inDecision))
                      case _ =>
                        flatten0(currDep, after, inDecision = true)
                    }
                }
              })

            case ErrorTo(node) =>
              accum get node match {
                case Some(graphNode) =>
                  graphNode errorTo = after.headOption
                  after foreach (_.before += graphNode)
                case _ =>
                  node match {
                    case Node(job: Job[_], deps) =>
                      val newNode = GraphNode(
                        job.jobName,
                        WorkflowJob(job)
                      )
                      newNode errorTo = after.headOption
                      after foreach (_.before += newNode)
                      accum += node -> newNode
                      deps foreach (flatten0(_, Set(newNode)))
                  }
              }

            case DecisionDependency(parent, option) =>
              val additionalDecisionRoute = (option getOrElse "default") -> parent
              after foreach (_.decisionRoutes += additionalDecisionRoute)
              flatten0(parent, after)

            case decisionNode@DecisionNode(decision, dependencies) =>
              val node = GraphNode(
                "decision",
                WorkflowDecision(decision.predicates, decisionNode),
                before = RefSet(),
                after = RefSet(),
                decisionAfter = RefSet(after.toSeq))
              node.name = "decision-" + node.getDecisionName(decision.predicates map (_._1))
              accum += currentDep -> node
              dependencies foreach { dep =>
                flatten0(dep, Set(node))
              }

            case _ =>
          }
      }

    }

    flatten0(workflow.end, Set.empty)
    val results = accum

    results.values foreach (node => {
      node.after foreach (_.before += node)
      node.decisionAfter foreach (_.decisionBefore += node)
    })
    updateDecisionNames(results.values.toList)
    verifyNames(results.values.toList)
    val additionalControlNodes = processForkJoins(results)
    fixLongNames(additionalControlNodes.values.toList)
    results ++ additionalControlNodes
  }

  def updateDecisionNames(nodes: List[GraphNode]) {
    val refSet = RefSet(nodes)
    val ordered = Conversion.order(refSet)
    val nodesToRename = ordered.toList sortBy (_.partialOrder) map (_.node) reverse

    nodesToRename foreach { node =>
      node.workflowOption match {
        case WorkflowDecision(predicates, decisionNode) =>
          node.name = "decision-" + node.getDecisionName(predicates map (_._1))
        case _ =>
      }
    }
  }

  def getDecisionLeaves(node: GraphNode, endNode: GraphNode): Set[GraphNode] = {
    if (node.after contains endNode)
      Set(node)
    else (Set[GraphNode]() /: node.after) (_ ++ getDecisionLeaves(_, endNode))
  }

  def isStartNode(node: GraphNode): Boolean = {
    node.before.isEmpty && node.decisionBefore.isEmpty
  }

  /*
* Will return true only for nodes that must lead to "end"
*/
  def isEndNode(node: GraphNode): Boolean = {
    (node.after.isEmpty || node.after.exists(_.workflowOption == WorkflowEnd)) && node.decisionAfter.isEmpty
  }

  /*
* Will return true for all nodes that may lead to "end"
*/
  def isPotentialEndNode(node: GraphNode): Boolean = {
    val isEnd = (node.after.isEmpty || node.after.exists(_.workflowOption == WorkflowEnd)) &&
      (node.decisionAfter.isEmpty || node.decisionAfter.exists(_.workflowOption == WorkflowEnd))
    isEnd
  }

  def processForkJoins(nodes: RefMap[Dependency, GraphNode]): RefMap[Dependency, GraphNode] = {
    var accum = RefMap[Dependency, GraphNode](Map.empty)

    def makeSuffix(nodes: RefSet[GraphNode]): String = {
      nodes.map((currNode: GraphNode) => currNode.name).toList.sorted mkString "-"
    }

    for (node <- nodes.values) {
      //check for forks in the graph
      if (node.after.size > 1) {
        val forkName = s"fork-${makeSuffix(node.after)}"
        val fork = GraphNode(forkName, WorkflowFork, RefSet(node), node.after)
        node.after = RefSet(fork)
        fork.after foreach { after => after.before = RefSet(fork) }
        accum += ForkDependency(forkName) -> fork
      }
      //check for joins in the graph
      if (node.before.size > 1) {
        val joinName = s"join-${makeSuffix(node.before)}"
        val join = GraphNode(joinName, WorkflowJoin, node.before, RefSet(node))
        node.before = RefSet(join)
        join.before foreach { before => before.after = RefSet(join) }
        accum += JoinDependency(joinName) -> join
      }
    }
    //check if we need a fork at the start of the workflow
    val firstNodes = RefSet((nodes.values.toSet filter isStartNode).toList)
    if (firstNodes.size > 1) {
      val forkName = s"fork-${makeSuffix(firstNodes)}"
      val fork = GraphNode(forkName, WorkflowFork, RefSet(), firstNodes)
      firstNodes foreach { after => after.before = RefSet(fork) }
      accum += ForkDependency(forkName) -> fork
    }
    //check if we need a join at the end of the workflow
    val lastNodes = RefSet((nodes.values.toSet filter isEndNode).toList)
    if (lastNodes.size > 1) {
      val joinName = s"join-${makeSuffix(lastNodes)}"
      val join = GraphNode(joinName, WorkflowJoin, lastNodes, RefSet())
      lastNodes foreach { before => before.after = RefSet(join) }
      accum += JoinDependency(joinName) -> join
    }
    accum
  }

  /*
* returns true if the two strings are the same,
* ignoring the last character of each if it is a digit
*/
  def nameNumMatch(name1: String, name2: String): Boolean = {
    val Pattern = """\d""".r

    def nameNumMatch0(n1: String, n2: String): Boolean = {
      n1.takeRight(1) match {
        case Pattern() =>
          n1.dropRight(1) == n2
        case _ =>
          n1 == n2
      }
    }

    nameNumMatch0(name1, name2) || nameNumMatch0(name2, name1)
  }

  def removeEndingDigits(nodes: List[GraphNode]): List[GraphNode] = {
    val Pattern = """\d""".r
    nodes map (currNode => {
      currNode.name.takeRight(1) match {
        case Pattern() => currNode.name = currNode.name.dropRight(1)
        case _ =>
      }
      currNode
    })
  }

  /*
* returns true if there exist at least 2 nodes in given list w/ same name
* ignoring the last character if it is a digit)
*/
  def hasDuplicates(nodes: List[GraphNode], name: String) = {
    (nodes filter (n => nameNumMatch(n.name, name))).size > 1
  }

  /*
* Performs verfication on node names, repairing incorrect names
*/
  def verifyNames(nodes: List[GraphNode]) = {
    removeDisallowedCharacters(nodes)
    fixDuplicateNames(nodes)
    fixLongNames(nodes)
  }

  /*
* - Removes disallowed characters such as "${}"
*/
  def removeDisallowedCharacters(nodes: List[GraphNode]) = {
    val Pattern = """[${}]""".r
    nodes foreach (n => {
      n.name = Pattern.replaceAllIn(n.name, "")
    })
  }

  /*
* Oozie nodes must have names of <= 50 characters
* Renames nodes with names of > 47 characters
*/
  def fixLongNames(nodes: List[GraphNode]): List[GraphNode] = {
    nodes foreach (n => {
      if (n.name.length > 47) {
        n.name = n.name.substring(0, 44) + "---"
      }
    })
    nodes
  }

  /*
* Renames nodes with duplicate names
*/
  def fixDuplicateNames(nodes: List[GraphNode]): List[GraphNode] = {
    val refSet = RefSet(nodes)
    val partiallyOrdered = Conversion.order(refSet)
    val orderedNodes: List[GraphNode] = partiallyOrdered.toList sortWith PartiallyOrderedNode.lt map (_.node)
    //get the nodes with duplicates in order from "bottom" to "top"
    var nodesToRename = orderedNodes.reverse filter (n => hasDuplicates(orderedNodes, n.name))
    nodesToRename = removeEndingDigits(nodesToRename)
    //rename
    nodesToRename foreach (node => {
      val sameNamedNodes = nodesToRename filter (n => n.name == node.name)
      val numSameNames: Int = sameNamedNodes.size
      if (numSameNames > 1) {
        node.name = node.name + numSameNames
        nodesToRename = nodesToRename filter (n => hasDuplicates(nodesToRename, n.name))
      }
    })
    nodes
  }
}