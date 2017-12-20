package com.github.izhangzhihao.soozie.verification

import com.github.izhangzhihao.soozie.conversion._

import scala.io.Source

case class VerificationNode(graphNode: GraphNode, parentThreads: Seq[ForkThread])

//path is the child of the fork that this thread lies on
case class ForkThread(fork: GraphNode, path: GraphNode)

object Verification {

  def verify(graph: Set[GraphNode]): Set[GraphNode] = {
    if (!verifyDecisions(graph))
      throw new RuntimeException("error: incorrect decision structure")
    val verifiedGraph = {
      if (!verifyForkJoins(graph)) {
        println("\nattempting to repair fork / joins\n")
        val fixedNodes = repairForkJoins(graph)
        Flatten.fixLongNames(fixedNodes.toList).toSet
      } else graph
    }
    verifiedGraph
  }

  def verifyDecisions(graph: Set[GraphNode]): Boolean = {
    val decisions: Set[GraphNode] = graph filter (node => node.workflowOption match {
      case dec@WorkflowDecision(predicates, _) => true
      case _ => false
    })
    //verify that each predicate is represented
    val verifiedDecisions = decisions filter (dec => {
      val (predicates, foundOptions) = dec.workflowOption match {
        case WorkflowDecision(predicates, myDecNode) =>
          val options = predicates.filter(currPred => {
            val containsPred = dec.decisionAfter.exists(_.decisionRoutes exists {
              case (route, decisionNode) => route == currPred._1 && decisionNode == myDecNode
            })
            containsPred
          })
          (predicates, options)
        case _ => throw new RuntimeException("error: unexpected type")
      }
      val optionsPresent = foundOptions.toSet == predicates.toSet
      val defaultPresent = dec.workflowOption match {
        case WorkflowDecision(predicates, myDecNode) =>
          dec.decisionAfter.exists(_.decisionRoutes exists {
            case (route, decisionNode) => route == "default" && decisionNode == myDecNode
          })
        case _ => throw new RuntimeException("error: unexpected type")
      }
      if (!defaultPresent)
        println(s"For decision '${dec.name}' missing 'default'")
      if (!optionsPresent)
        println(s"For decision '${dec.name}' missing options. needed '$predicates' but found '$foundOptions'")
      optionsPresent && defaultPresent
    })
    verifiedDecisions.size == decisions.size
  }

  /*
* From the Oozie spec:
* "The fork and join nodes must be used in pairs.
* The join node assumes concurrent execution paths are children of the same fork node."
* (http://oozie.apache.org/docs/3.2.0-incubating/WorkflowFunctionalSpec.html#a3.1.5_Fork_and_Join_Control_Nodes)
*/
  def verifyForkJoins(graph: Set[GraphNode]): Boolean = {
    getBadJoins(graph) isEmpty
  }

  /*
* Reworks graph to create workflow that oozie will allow
* Should only be used if verifyForkJoins fails
*/
  def repairForkJoins(graph: Set[GraphNode], repairAutomatically: Boolean = false, defaultPath: String = "${outputDataRoot}"): Set[GraphNode] = {
    val ln = Source.stdin.getLines
    if (!repairAutomatically) {
      println("worflow fork/join structure not allowed by oozie runtime - repair? (y/n)")
      val repair = ln.next
      repair match {
        case "y" =>
        case _ => throw new RuntimeException("error: fork / join structure not supported by oozie")
      }
    }
    var resultGraph = graph
    while (getBadJoins(resultGraph).nonEmpty) {
      val startNodes = resultGraph filter (n => Flatten.isStartNode(n))
      val vNodes = generateVerificationNodes(startNodes)
      val badJoin: VerificationNode = getFirstBadJoin(resultGraph)
      //try just adding a join - simple case
      val tryResult = addJoin(badJoin, resultGraph, vNodes)
      if (tryResult != resultGraph) {
        resultGraph = tryResult
      } //otherwise, need to move nodes around
      else {
        //For now just error, don't try to move nodes around
        println("unable to repair by adding joins - must modify workflow structure")
        throw new RuntimeException("error: fork / join structure not supported by oozie")
      }
    }
    resultGraph
  }

  /*
* Returns a set of joins that violate Oozie's fork/join pair requirement
*/
  def getBadJoins(graph: Set[GraphNode]): Set[VerificationNode] = {

    var badJoins: RefSet[VerificationNode] = RefSet()

    //mapping of join -> parent fork
    var joins: Map[GraphNode, GraphNode] = Map.empty

    def getBadJoins0(currentNode: VerificationNode): Unit = {
      val currNode = currentNode
      currNode.graphNode.workflowOption match {
        //if fork, we're on a new parent thread. Push this thread onto the stack
        case WorkflowFork =>
          currNode.graphNode.after foreach (n => getBadJoins0(VerificationNode(n, currNode.parentThreads :+ ForkThread(currNode.graphNode, n))))
        //if join, we've closed a thread, so pop the thread off the stack
        case WorkflowJoin => {
          //validate that all nodes coming into the join are on same thread
          joins.get(currNode.graphNode) match {
            case Some(node) =>
              if (node != currNode.parentThreads.last.fork) {
                badJoins += currNode
              }
            case _ => joins += currNode.graphNode -> currNode.parentThreads.last.fork
          }
          currNode.graphNode.after foreach (n => getBadJoins0(VerificationNode(n, currNode.parentThreads.init)))
        }
        case _ =>
          (currNode.graphNode.after ++ currNode.graphNode.decisionAfter) foreach (n => getBadJoins0(VerificationNode(n, currNode.parentThreads)))
      }
    }

    val startNodes = graph filter (n => Flatten.isStartNode(n))
    startNodes foreach (n => getBadJoins0(VerificationNode(n, Seq(ForkThread(n, n)))))
    badJoins
  }

  /*
* Generates a graph of VerificationNodes from a graph of GraphNodes
*/
  def generateVerificationNodes(startNodes: Set[GraphNode]): Set[VerificationNode] = {

    def generateVerificationNodes0(currNode: VerificationNode): Set[VerificationNode] = {
      val newParentThreads = currNode.graphNode.workflowOption match {
        case WorkflowFork =>
          currNode.parentThreads :+ ForkThread(currNode.graphNode, currNode.graphNode.after.head)
        case WorkflowJoin =>
          currNode.parentThreads.init
        case _ =>
          currNode.parentThreads
      }
      (Set(currNode) /: currNode.graphNode.after) ((e1, e2) => e1 ++ generateVerificationNodes0(VerificationNode(e2, newParentThreads)))
    }

    (Set[VerificationNode]() /: startNodes) ((e1, e2) => e1 ++ generateVerificationNodes0(VerificationNode(e2, Seq(ForkThread(e2, e2)))))
  }

  /*
* Get bad join that is closest to the "top" of the workflow tree
*/
  def getFirstBadJoin(graph: Set[GraphNode]): VerificationNode = {

    val badJoins = getBadJoins(graph)
    val badJoinsGraphNodes = badJoins map (_.graphNode)
    //work "down" through the graph do get the first bad join
    val orderedGraph: Set[PartiallyOrderedNode] = Conversion.order(RefSet(graph.toSeq))
    val partiallyOrderedBadJoins = orderedGraph filter (n => badJoinsGraphNodes contains n.node)
    val orderedBadJoinsGraphNodes: List[GraphNode] = partiallyOrderedBadJoins.toList sortWith PartiallyOrderedNode.lt map (_.node)
    val orderedBadJoins: List[VerificationNode] = orderedBadJoinsGraphNodes map (n => VerificationNode(n, badJoins.find(gn => gn.graphNode == n).get.parentThreads))
    orderedBadJoins.head
  }

  /*
* If a bad join can be fixed by simply adding another join, does so.
* If not, returns input graph
*/
  def addJoin(badJoin: VerificationNode, graph: Set[GraphNode], vNodes: Set[VerificationNode]): Set[GraphNode] = {
    var resultGraph = graph
    val parentVNodes: Set[VerificationNode] = badJoin.graphNode.before map ((currNode: GraphNode) => getVNode(vNodes, currNode))
    //build up the set of threads that are coming into this bad join
    val badJoinParentForks: Set[GraphNode] = parentVNodes map (_.parentThreads.last.fork)
    //find the first node that has the same parent fork as others
    val firstNodeToJoin: Option[VerificationNode] = parentVNodes find (n => (parentVNodes count (n2 => n2.parentThreads.last.fork == n.parentThreads.last.fork)) > 1)

    firstNodeToJoin match {
      case Some(headNode) => //then simply join the nodes on the same thread
        val joinThread: GraphNode = headNode.parentThreads.last.fork
        val toJoin: Set[GraphNode] = parentVNodes filter (n => n.parentThreads.last.fork == joinThread) map (_.graphNode)
        //insert into graph
        val newJoin = GraphNode(
          "join" + {
            var nameStr = ""
            toJoin foreach (nameStr += "-" + _.name)
            nameStr
          },
          workflowOption = WorkflowJoin,
          before = RefSet(toJoin.toSeq),
          after = RefSet(badJoin.graphNode))
        //update before/after
        resultGraph += newJoin
        badJoin.graphNode.before --= RefSet(toJoin.toSeq)
        badJoin.graphNode.before += newJoin
        toJoin foreach (n => n.after = RefSet(newJoin))
      case _ => //otherwise, this can't be fixed by adding joins - Do nothing.
    }
    resultGraph
  }

  /*
* Requires: node's VerificationNode representation is contained in vNodes
*
* Returns the VerificationNode representation of node
*/
  def getVNode(vNodes: Set[VerificationNode], node: GraphNode): VerificationNode = {

    vNodes.find(n => n.graphNode == node).getOrElse(throw new RuntimeException("target node not contained in set of verification nodes"))
  }
}