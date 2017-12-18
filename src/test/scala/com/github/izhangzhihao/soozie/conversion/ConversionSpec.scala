package com.github.izhangzhihao.soozie.conversion

import com.github.izhangzhihao.soozie.dsl._
import com.github.izhangzhihao.soozie.jobs.MapReduceJob
import oozie.workflow_0_5._
import org.specs2.mutable._
import oozie.XMLProtocol._
import scalaxb._

class ConversionSpec extends Specification {
    "Conversion" should {
        "give empty result for empty Workflow" in {
            val wf = WORKFLOWu45APP(
                name = "empty",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("kill"), KILL(
                        message = "empty" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("end"),
                end = END("end"))
            Conversion(EmptyWorkflow) must_== wf
        }

        "give Workflow with single node for single Workflow" in {
            val wf = WORKFLOWu45APP(
                name = "single",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_start",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "single" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_start"),
                end = END("end"))

            Conversion(SingleWorkflow) must_== wf
        }

        "give workflow with 4 sequential jobs" in {
            val wf = WORKFLOWu45APP(
                name = "simple",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_second",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_third"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_third",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_fourth"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_fourth",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "simple" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(SimpleWorkflow) must_== wf
        }

        "give workflow with 2 jobs running in parallel" in {
            val wf = WORKFLOWu45APP(
                name = "simple-fork-join",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_secondA"),
                            FORK_TRANSITION("mr_secondB")),
                        name = "fork-mr_secondA-mr_secondB")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_secondA",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_secondB",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(
                        name = "join-mr_secondA-mr_secondB",
                        to = "end")),
                    DataRecord(None, Some("kill"), KILL(
                        message = "simple-fork-join" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(SimpleForkJoin) must_== wf
        }

        "give workflow with 1 job, decision, then two more jobs" in {
            val wf = WORKFLOWu45APP(
                name = "simple-decision",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("decision-mr_default-mr_option"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "true",
                                        to = "mr_option")),
                                default = DEFAULT(
                                    to = "mr_default"))),
                        name = "decision-mr_default-mr_option")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_default",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_option",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_second",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "simple-decision" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(SimpleDecision) must_== wf
        }

        "give workflow with 1 job, then sub workflow, then 1 more job" in {
            val wf = WORKFLOWu45APP(
                name = "simple-sub-workflow",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_begin",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_first"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_second",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_third"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_third",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_fourth"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_fourth",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_final"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_final",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "simple-sub-workflow" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_begin"),
                end = END("end"))

            Conversion(SimpleSubWorkflow) must_== wf
        }

        "give workflow with two separate fork / joins" in {
            val wf = WORKFLOWu45APP(
                name = "two-simple-fork-joins",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_secondA"),
                            FORK_TRANSITION("mr_secondB")),
                        name = "fork-mr_secondA-mr_secondB")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_secondA",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_secondB",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(
                        name = "join-mr_secondA-mr_secondB",
                        to = "mr_third")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_third",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_fourthA-mr_fourthB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_fourthA"),
                            FORK_TRANSITION("mr_fourthB")),
                        name = "fork-mr_fourthA-mr_fourthB")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_fourthA",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_fourthA-mr_fourthB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_fourthB",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_fourthA-mr_fourthB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(
                        name = "join-mr_fourthA-mr_fourthB",
                        to = "end")),
                    DataRecord(None, Some("kill"), KILL(
                        message = "two-simple-fork-joins" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(TwoSimpleForkJoins) must_== wf
        }

        "give workflow with nested fork / joins" in {
            val wf = WORKFLOWu45APP(
                name = "nested-fork-join",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_secondA-mr_secondB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_secondA"),
                            FORK_TRANSITION("mr_secondB")),
                        name = "fork-mr_secondA-mr_secondB")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_secondA",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_secondB",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_thirdC"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_thirdA"),
                            FORK_TRANSITION("mr_thirdB")),
                        name = "fork-mr_thirdA-mr_thirdB")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_thirdC",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_fourth-mr_thirdC"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_thirdA",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_thirdB",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(
                        name = "join-mr_thirdA-mr_thirdB",
                        to = "mr_fourth")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_fourth",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_fourth-mr_thirdC"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(
                        name = "join-mr_fourth-mr_thirdC",
                        to = "end")),
                    DataRecord(None, Some("kill"), KILL(
                        message = "nested-fork-join" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(NestedForkJoin) must_== wf
        }

        "give workflow with subworkflow, including fork joins" in {
            val wf = WORKFLOWu45APP(
                name = "sub-fork-join",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_start",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_first"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_second",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_third"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_third",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_fourth"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_fourth",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_thirdA"),
                            FORK_TRANSITION("mr_thirdB")),
                        name = "fork-mr_thirdA-mr_thirdB")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_thirdA",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_thirdB",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_thirdA-mr_thirdB"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(
                        name = "join-mr_thirdA-mr_thirdB",
                        to = "end")),
                    DataRecord(None, Some("kill"), KILL(
                        message = "sub-fork-join" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_start"),
                end = END("end"))

            Conversion(SubworkflowWithForkJoins) must_== wf
        }

        "give workflow with duplicate nodes" in {
            val wf = WORKFLOWu45APP(
                name = "duplicate-nodes",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("fork-mr_second-mr_second2"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("fork"), FORK(
                        path = Seq(
                            FORK_TRANSITION("mr_second"),
                            FORK_TRANSITION("mr_second2")),
                        name = "fork-mr_second-mr_second2")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_second",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_second-mr_second2"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_second2",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("join-mr_second-mr_second2"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("join"), JOIN(
                        name = "join-mr_second-mr_second2",
                        to = "end")),
                    DataRecord(None, Some("kill"), KILL(
                        message = "duplicate-nodes" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(DuplicateNodes) must_== wf
        }

        "give workflow with syntactically sugared decision" in {
            val wf = WORKFLOWu45APP(
                name = "sugar-option-decision",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("decision-mr_second-mr_option"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "${doOption}",
                                        to = "mr_option")),
                                default = DEFAULT(
                                    to = "mr_second"))),
                        name = "decision-mr_second-mr_option")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_option",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_second",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "sugar-option-decision" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(SugarOption) must_== wf
        }

        "give workflow with regular decision and syntactically sugared decision" in {
            val wf = WORKFLOWu45APP(
                name = "mixed-decision-styles",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("decision-mr_default2-decision-mr_default-mr_---"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "true",
                                        to = "decision-mr_default-mr_option")),
                                default = DEFAULT(
                                    to = "mr_default2"))),
                        name = "decision-mr_default2-decision-mr_default-mr_---")),
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "${doOption}",
                                        to = "mr_option")),
                                default = DEFAULT(
                                    to = "mr_default"))),
                        name = "decision-mr_default-mr_option")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_option",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_default"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_default",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_default2"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_default2",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "mixed-decision-styles" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(DecisionAndSugarOption) must_== wf
        }

        "give workflow with custom error-to message" in {
            val wf = WORKFLOWu45APP(
                name = "custom-errorTo",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_first",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_second"),
                        error = ACTION_TRANSITION("mr_errorOption"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_errorOption",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_second",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "custom-errorTo" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("mr_first"),
                end = END("end"))

            Conversion(CustomErrorTo) must_== wf
        }

        "give workflow with sub-workflow ending in sugar decision option" in {
            val wf = WORKFLOWu45APP(
                name = "sub-wf-ending-with-sugar-option",
                workflowu45appoption = Seq(
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "${doOption}",
                                        to = "mr_option")),
                                default = DEFAULT(
                                    to = "mr_default"))),
                        name = "decision-mr_default-mr_option")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_default",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("decision-mr_last-mr_sugarOption"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_option",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_last"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("decision"), DECISION(
                        switch = SWITCH(
                            switchsequence1 = SWITCHSequence1(
                                caseValue = Seq(
                                    CASE(
                                        value = "${doSugarOption}",
                                        to = "mr_sugarOption")),
                                default = DEFAULT(
                                    to = "mr_last"))),
                        name = "decision-mr_last-mr_sugarOption")),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_sugarOption",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("mr_last"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("action"), ACTION(
                        name = "mr_last",
                        actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                            jobu45tracker = Some("${jobTracker}"),
                            nameu45node = Some("${nameNode}"))),
                        ok = ACTION_TRANSITION("end"),
                        error = ACTION_TRANSITION("kill"))),
                    DataRecord(None, Some("kill"), KILL(
                        message = "sub-wf-ending-with-sugar-option" + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]",
                        name = "kill"))),
                start = START("decision-mr_default-mr_option"),
                end = END("end"))
            Conversion(SubWfEndWithSugarOption) must_== wf
        }

    }

    object TestMapReduceJob {
        def apply(name: String) = {
            MapReduceJob(
                jobName = s"mr_$name",
                nameNode = Some("${nameNode}"),
                jobTracker = Some("${jobTracker}"))
        }
    }

    def EmptyWorkflow = {
        val end = End dependsOn Nil
        Workflow("empty", end)
    }

    def SingleWorkflow = {
        val start = TestMapReduceJob("start") dependsOn Start
        val end = End dependsOn start
        Workflow("single", end)
    }

    def SimpleWorkflow = {
        val first = TestMapReduceJob("first") dependsOn Start
        val second = TestMapReduceJob("second") dependsOn first
        val third = TestMapReduceJob("third") dependsOn second
        val fourth = TestMapReduceJob("fourth") dependsOn third
        val end = End dependsOn fourth
        Workflow("simple", end)
    }

    def SimpleForkJoin = {
        val first = TestMapReduceJob("first") dependsOn Start
        val secondA = TestMapReduceJob("secondA") dependsOn first
        val secondB = TestMapReduceJob("secondB") dependsOn first
        val end = End dependsOn (secondA, secondB)
        Workflow("simple-fork-join", end)
    }

    def SimpleDecision = {
        val first = TestMapReduceJob("first") dependsOn Start
        val decision = Decision("route1" -> Predicates.AlwaysTrue) dependsOn first //decision is a DecisionNode
        val default = TestMapReduceJob("default") dependsOn (decision default)
        val option = TestMapReduceJob("option") dependsOn (decision option "route1")
        val second = TestMapReduceJob("second") dependsOn OneOf(default, option)
        val done = End dependsOn second
        Workflow("simple-decision", done)
    }

    def SimpleSubWorkflow = {
        val first = TestMapReduceJob("begin") dependsOn Start
        val subWf = SimpleWorkflow dependsOn first
        val third = TestMapReduceJob("final") dependsOn subWf
        val end = End dependsOn third
        Workflow("simple-sub-workflow", end)
    }

    def TwoSimpleForkJoins = {
        val first = TestMapReduceJob("first") dependsOn Start
        val secondA = TestMapReduceJob("secondA") dependsOn first
        val secondB = TestMapReduceJob("secondB") dependsOn first
        val third = TestMapReduceJob("third") dependsOn (secondA, secondB)
        val fourthA = TestMapReduceJob("fourthA") dependsOn third
        val fourthB = TestMapReduceJob("fourthB") dependsOn third
        val end = End dependsOn (fourthA, fourthB)
        Workflow("two-simple-fork-joins", end)
    }

    def NestedForkJoin = {
        val first = TestMapReduceJob("first") dependsOn Start
        val secondA = TestMapReduceJob("secondA") dependsOn first
        val secondB = TestMapReduceJob("secondB") dependsOn first
        val thirdA = TestMapReduceJob("thirdA") dependsOn secondA
        val thirdB = TestMapReduceJob("thirdB") dependsOn secondA
        val thirdC = TestMapReduceJob("thirdC") dependsOn secondB
        val fourth = TestMapReduceJob("fourth") dependsOn (thirdA, thirdB)
        val end = End dependsOn (fourth, thirdC)
        Workflow("nested-fork-join", end)
    }

    def SubworkflowWithForkJoins = {
        val start = TestMapReduceJob("start") dependsOn Start
        val sub = SimpleWorkflow dependsOn start
        val thirdA = TestMapReduceJob("thirdA") dependsOn sub
        val thirdB = TestMapReduceJob("thirdB") dependsOn sub
        val end = End dependsOn (thirdA, thirdB)
        Workflow("sub-fork-join", end)
    }

    def DuplicateNodes = {
        val first = TestMapReduceJob("first") dependsOn Start
        val second = TestMapReduceJob("second") dependsOn first
        val third = TestMapReduceJob("second") dependsOn first
        val end = End dependsOn (second, third)
        Workflow("duplicate-nodes", end)
    }

    def SugarOption = {
        val first = TestMapReduceJob("first") dependsOn Start
        val option = TestMapReduceJob("option") dependsOn first doIf "${doOption}"
        val second = TestMapReduceJob("second") dependsOn Optional(option)
        val done = End dependsOn second
        Workflow("sugar-option-decision", done)
    }

    def DecisionAndSugarOption = {
        val first = TestMapReduceJob("first") dependsOn Start
        val decision = Decision(
            "route1" -> Predicates.AlwaysTrue
        ) dependsOn first
        val option = TestMapReduceJob("option") dependsOn (decision option "route1") doIf "${doOption}"
        val default = TestMapReduceJob("default") dependsOn Optional(option)
        val default2 = TestMapReduceJob("default2") dependsOn OneOf(decision default, default)
        val end = End dependsOn default2
        Workflow("mixed-decision-styles", end)
    }

    def CustomErrorTo = {
        val first = TestMapReduceJob("first") dependsOn Start
        val errorOption = TestMapReduceJob("errorOption") dependsOn (first error)
        val second = TestMapReduceJob("second") dependsOn first
        val end = End dependsOn OneOf(second, errorOption)
        Workflow("custom-errorTo", end)
    }

    def SubWfEndWithSugarOption = {

        val wf = WfEndWithSugarOption dependsOn Start
        val last = TestMapReduceJob("last") dependsOn wf
        val end = End dependsOn last
        Workflow("sub-wf-ending-with-sugar-option", end)
    }

    def WfEndWithSugarOption = {

        val decision = Decision(
            "option" -> Predicates.BooleanProperty("doOption")
        ) dependsOn Start
        val option = TestMapReduceJob("option") dependsOn (decision option "option")
        val default = TestMapReduceJob("default") dependsOn (decision default)
        val sugarOption = TestMapReduceJob("sugarOption") dependsOn default doIf "doSugarOption"
        val end = End dependsOn OneOf(option, Optional(sugarOption))
        Workflow("test-agg-content", end)
    }

}