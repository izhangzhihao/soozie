package com.github.izhangzhihao.soozie.verification

import com.github.izhangzhihao.soozie.conversion._
import com.github.izhangzhihao.soozie.dsl._
import com.github.izhangzhihao.soozie.examples.SimpleSamples._
import org.specs2.mutable._

class VerificationSpec extends Specification {

    "Verification" should {

        //unit tests for verifyForkJoins
        "give verified for empty graph" in {
            Verification.verifyForkJoins(Flatten(EmptyWorkflow).values.toSet) must beTrue
        }

        "give verified for simple graph" in {
            Verification.verifyForkJoins(Flatten(SimpleWorkflow).values.toSet) must beTrue
        }

        "give verified for simple fork/join" in {
            Verification.verifyForkJoins(Flatten(SimpleForkJoin).values.toSet) must beTrue
        }

        "give verified for two simple fork/joins" in {
            Verification.verifyForkJoins(Flatten(TwoSimpleForkJoins).values.toSet) must beTrue
        }

        "give unverified for graph with joins from different parent forks" in {
            Verification.verifyForkJoins(Flatten(NestedForkJoinFs).values.toSet) must beFalse
        }

        "give verified for graph with joins inserted to make final join from same parent thread" in {
            Verification.verifyForkJoins(Flatten(NestedForkJoinFs2).values.toSet) must beTrue
        }

        "give unverified for graph with joins from different parent forks, but same grandparent fork" in {
            Verification.verifyForkJoins(Flatten(NestedForkJoinFs3).values.toSet) must beFalse
        }

        "give unverified for graph with same # of forks and joins, but joins from different parent forks" in {
            Verification.verifyForkJoins(Flatten(NestedForkJoinFs4).values.toSet) must beFalse
        }

        "give verified for graph with simple fork / join and decisions" in {
            Verification.verifyForkJoins(Flatten(SimpleDecisionForkJoin).values.toSet) must beTrue
        }

        //unit tests for repairForkJoins
        "give repaired graph by adding join for oozie-disallowed input workflow" in {

            val first = GraphNode("first", WorkflowJob(NoOpJob("first")))
            val firstFork = GraphNode("fork-secondA-secondB", WorkflowFork)
            val secondA = GraphNode("secondA", WorkflowJob(NoOpJob("secondA")))
            val secondB = GraphNode("secondB", WorkflowJob(NoOpJob("secondB")))
            val secondFork = GraphNode("fork-thirdA-thirdB", WorkflowFork)
            val thirdA = GraphNode("thirdA", WorkflowJob(NoOpJob("thirdA")))
            val thirdB = GraphNode("thirdB", WorkflowJob(NoOpJob("thirdB")))
            val thirdC = GraphNode("thirdC", WorkflowJob(NoOpJob("thirdC")))
            val firstJoin = GraphNode("join-thirdB-thirdA", WorkflowJoin)
            val secondJoin = GraphNode("join-thirdA-thirdB-thirdC", WorkflowJoin)
            val fourth = GraphNode("fourth", WorkflowJob(NoOpJob("fourth")))
            val end = GraphNode("end", WorkflowEnd)

            first.after = RefSet(firstFork)
            firstFork.before = RefSet(first)
            firstFork.after = RefSet(secondA, secondB)
            secondA.before = RefSet(firstFork)
            secondA.after = RefSet(secondFork)
            secondB.before = RefSet(firstFork)
            secondB.after = RefSet(thirdC)
            secondFork.before = RefSet(secondA)
            secondFork.after = RefSet(thirdA, thirdB)
            thirdA.before = RefSet(secondFork)
            thirdA.after = RefSet(firstJoin)
            thirdB.before = RefSet(secondFork)
            thirdB.after = RefSet(firstJoin)
            thirdC.before = RefSet(secondB)
            thirdC.after = RefSet(secondJoin)
            firstJoin.before = RefSet(thirdA, thirdB)
            firstJoin.after = RefSet(secondJoin)
            secondJoin.before = RefSet(firstJoin, thirdC)
            secondJoin.after = RefSet(fourth)
            fourth.before = RefSet(secondJoin)
            fourth.after = RefSet(end)

            val graphNodes = Flatten(OozieNotAllowed2).values.toSet

            Verification.repairForkJoins(graphNodes, repairAutomatically = true) must_== Set(first, firstFork, secondA, secondB, secondFork, thirdA, thirdB, thirdC, firstJoin, secondJoin, fourth)
        }.pendingUntilFixed("This wasn't working on project import")

        "give repaired graph by adding 2 joins for oozie-disallowed input workflow" in {

            val first = GraphNode("first", WorkflowJob(NoOpJob("first")))
            val firstFork = GraphNode("fork-secondA-secondB", WorkflowFork)
            val secondA = GraphNode("secondA", WorkflowJob(NoOpJob("secondA")))
            val secondB = GraphNode("secondB", WorkflowJob(NoOpJob("secondB")))
            val secondFork = GraphNode("fork-thirdA-thirdB", WorkflowFork)
            val thirdFork = GraphNode("fork-thirdC-thirdD", WorkflowFork)
            val thirdA = GraphNode("thirdA", WorkflowJob(NoOpJob("thirdA")))
            val thirdB = GraphNode("thirdB", WorkflowJob(NoOpJob("thirdB")))
            val thirdC = GraphNode("thirdC", WorkflowJob(NoOpJob("thirdC")))
            val thirdD = GraphNode("thirdD", WorkflowJob(NoOpJob("thirdD")))
            val firstJoin = GraphNode("join-thirdA-thirdB", WorkflowJoin)
            val secondJoin = GraphNode("join-thirdC-thirdD", WorkflowJoin)
            val thirdJoin = GraphNode("join-thirdA-thirdB-thirdC-thirdD", WorkflowJoin)

            first.after = RefSet(firstFork)
            firstFork.before = RefSet(first)
            firstFork.after = RefSet(secondA, secondB)
            secondA.before = RefSet(firstFork)
            secondA.after = RefSet(secondFork)
            secondB.before = RefSet(firstFork)
            secondB.after = RefSet(thirdFork)
            secondFork.before = RefSet(secondA)
            secondFork.after = RefSet(thirdA, thirdB)
            thirdFork.before = RefSet(secondB)
            thirdFork.after = RefSet(thirdC, thirdD)
            thirdA.before = RefSet(secondFork)
            thirdA.after = RefSet(firstJoin)
            thirdB.before = RefSet(secondFork)
            thirdB.after = RefSet(firstJoin)
            thirdC.before = RefSet(thirdFork)
            thirdC.after = RefSet(secondJoin)
            thirdD.before = RefSet(thirdFork)
            thirdD.after = RefSet(secondJoin)
            firstJoin.before = RefSet(thirdA, thirdB)
            firstJoin.after = RefSet(thirdJoin)
            secondJoin.before = RefSet(thirdC, thirdD)
            secondJoin.after = RefSet(thirdJoin)
            thirdJoin.before = RefSet(firstJoin, secondJoin)

            val graphNodes = Flatten(OozieNotAllowed3).values.toSet

            Verification.repairForkJoins(graphNodes, repairAutomatically = true) must_== Set(first, firstFork, secondFork, thirdFork, secondA, secondB, thirdA, thirdB, thirdC, thirdD, firstJoin, secondJoin, thirdJoin)
        }.pendingUntilFixed("This wasn't working on project import")

        //unit tests for verifying decisions
        "give valid result for simple decision" in {
            Verification.verifyDecisions(Flatten(simpleValidDecision).values.toSet) must beTrue
        }

        "give invalid result for invalid simple decision" in {
            Verification.verifyDecisions(Flatten(simpleInvalidDecision).values.toSet) must beFalse
        }

        "give invalid result for second invalid simple decision" in {
            Verification.verifyDecisions(Flatten(simpleInvalidDecision2).values.toSet) must beFalse
        }

        "give valid for both decision nodes going to end" in {
            def complexDecisions = {
                val first = Decision(
                    "foo" -> Predicates.BooleanProperty("bar")
                ) dependsOn Start
                val end = End dependsOn OneOf(first default, first option "foo")
                Workflow("complex-decisions", end)
            }
            Verification.verifyDecisions(Flatten(complexDecisions).values.toSet) must beTrue
        }

        "give valid for both decision nodes going to same place not end" in {
            def ComplexDecision = {
                val first = Decision(
                    "foo" -> Predicates.BooleanProperty("bar")
                ) dependsOn Start
                val foo = NoOpJob("foo2") dependsOn OneOf(first default, first option "foo")
                val end = End dependsOn foo
                Workflow("complex-decisions", end)
            }
            Verification.verifyDecisions(Flatten(ComplexDecision).values.toSet) must beTrue
        }

        "give valid for decision with one route at end" in {
            def decision = {
                val first = Decision(
                    "foo" -> Predicates.BooleanProperty("bar")
                ) dependsOn Start
                val foo = NoOpJob("foo") dependsOn (first option "foo")
                val end = End dependsOn OneOf(foo, first default)
                Workflow("complex-decisions", end)
            }
            Verification.verifyDecisions(Flatten(decision).values.toSet) must beTrue

            def decision2 = {
                val first = Decision(
                    "foo" -> Predicates.BooleanProperty("bar")
                ) dependsOn Start
                val foo = NoOpJob("foo") dependsOn (first default)
                val end = End dependsOn OneOf(foo, first option "foo")
                Workflow("complex-decisions", end)
            }
            Verification.verifyDecisions(Flatten(decision2).values.toSet) must beTrue
        }

    }

    def SimpleForkJoin = {
        val first = NoOpJob("first") dependsOn Start
        val second = NoOpJob("second") dependsOn Start
        val end = End dependsOn (first, second)
        Workflow("simple-fj", end)
    }

    def SimpleDecisionForkJoin = {
        val first = NoOpJob("first") dependsOn Start
        val decision1 = Decision("route1" -> Predicates.AlwaysTrue) dependsOn first //decision is a DecisionNode
        val default1 = NoOpJob("default1") dependsOn (decision1 default)
        val option1 = NoOpJob("option1") dependsOn (decision1 option "route1")
        val decision2 = Decision("route1" -> Predicates.AlwaysTrue) dependsOn first
        val default2 = NoOpJob("default2") dependsOn (decision2 default)
        val option2 = NoOpJob("option2") dependsOn (decision2 option "route1")
        val second1 = NoOpJob("second1") dependsOn OneOf(default1, option1)
        val second2 = NoOpJob("second2") dependsOn OneOf(default2, option2)
        val done = End dependsOn (second1, second2)
        Workflow("simple-decision", done)
    }

    def OozieNotAllowed = {
        val first = NoOpJob("first") dependsOn Start
        val secondA = NoOpJob("secondA") dependsOn first
        val secondB = NoOpJob("secondB") dependsOn first
        val thirdA = NoOpJob("thirdA") dependsOn secondA
        val thirdB = NoOpJob("thirdB") dependsOn secondA
        val thirdC = NoOpJob("thirdC") dependsOn secondB
        val fourthA = NoOpJob("fourthA") dependsOn thirdA
        val fourthB = NoOpJob("fourthB") dependsOn (thirdB, thirdC)
        val fifth = NoOpJob("fifth") dependsOn (fourthA, fourthB)
        val end = End dependsOn fifth
        Workflow("not-allowed", end)
    }

    def OozieNotAllowed2 = {
        val first = NoOpJob("first") dependsOn Start
        val secondA = NoOpJob("secondA") dependsOn first
        val secondB = NoOpJob("secondB") dependsOn first
        val thirdA = NoOpJob("thirdA") dependsOn secondA
        val thirdB = NoOpJob("thirdB") dependsOn secondA
        val thirdC = NoOpJob("thirdC") dependsOn secondB
        val fourth = NoOpJob("fourth") dependsOn (thirdA, thirdB, thirdC)
        val end = End dependsOn fourth
        Workflow("not-allowed-2", end)
    }

    def OozieNotAllowed3 = {
        val first = NoOpJob("first") dependsOn Start
        //fork-secondA-secondB
        val secondA = NoOpJob("secondA") dependsOn first
        val secondB = NoOpJob("secondB") dependsOn first
        //fork-thirdA-thirdB
        val thirdA = NoOpJob("thirdA") dependsOn secondA
        val thirdB = NoOpJob("thirdB") dependsOn secondA
        //fork-thirdC-thirdD
        val thirdC = NoOpJob("thirdC") dependsOn secondB
        val thirdD = NoOpJob("thirdD") dependsOn secondB
        //join
        val end = End dependsOn (thirdA, thirdB, thirdC, thirdD)
        Workflow("not-allowed-3", end)
    }

    def OozieNotAllowed4 = {
        val first = NoOpJob("first") dependsOn Start
        val secondA = NoOpJob("secondA") dependsOn first
        val secondB = NoOpJob("secondB") dependsOn first
        val thirdA = NoOpJob("thirdA") dependsOn secondA
        val thirdB = NoOpJob("thirdB") dependsOn secondA
        val fourth = NoOpJob("fourth") dependsOn (thirdB, secondB)
        val end = End dependsOn (thirdA, fourth)
        Workflow("not-allowed-4", end)
    }

    def simpleValidDecision = {
        val first = NoOpJob("first") dependsOn Start
        val dec = Decision("route1" -> Predicates.AlwaysTrue) dependsOn first
        val default = NoOpJob("default") dependsOn (dec default)
        val option = NoOpJob("option") dependsOn (dec option "route1")
        val second = NoOpJob("second") dependsOn OneOf(default, option)
        Workflow("simple-valid-decision", second)
    }

    def simpleInvalidDecision = {
        val first = NoOpJob("first") dependsOn Start
        val dec = Decision("route1" -> Predicates.AlwaysTrue) dependsOn first
        val option = NoOpJob("option") dependsOn (dec option "route1")
        val second = NoOpJob("second") dependsOn option
        Workflow("simple-valid-decision", second)
    }

    def simpleInvalidDecision2 = {
        val first = NoOpJob("first") dependsOn Start
        val dec = Decision("route1" -> Predicates.AlwaysTrue) dependsOn first
        val default = NoOpJob("default") dependsOn (dec default)
        val second = NoOpJob("second") dependsOn default
        Workflow("simple-valid-decision2", second)
    }
}