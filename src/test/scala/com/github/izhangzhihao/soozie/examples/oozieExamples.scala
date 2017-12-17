package com.github.izhangzhihao.soozie.examples

import oozie.XMLProtocol._
import oozie.workflow._
import scalaxb._

/* 
 * Example from the Oozie Website:
 * "The following workflow definition example executes 4 Map-Reduce jobs in 3 steps -
 * 1 job, 2 jobs in parallel and 1 job."
 * (http://oozie.apache.org/docs/3.3.2/WorkflowFunctionalSpec.html#a3.2.6_Sub-workflow_Action)
 * translated to Soozie.
 */
object WfExample {
    val nodes: Seq[DataRecord[WORKFLOWu45APPOption]] = Seq(
        DataRecord(None, Some("action"), ACTION(
            name = "firstjob",
            actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                jobu45tracker = Some("${jobTracker}"),
                nameu45node = Some("${nameNode}"),
                configuration = Some(CONFIGURATION(Seq(
                    Property2("mapred.mapper.class", "org.apache.hadoop.example.IdMapper"),
                    Property2("mapred.reducer.class", "org.apache.hadoop.example.IdReducer"),
                    Property2("mapred.map.tasks", "1"),
                    Property2("mapred.input.dir", "${input}"),
                    Property2("mapred.output.dir", "/usr/foo/${wf:id()}/temp1")))))),
            ok = ACTION_TRANSITION("fork"),
            error = ACTION_TRANSITION("kill"))),
        DataRecord(None, Some("fork"), FORK(
            path = Seq(
                FORK_TRANSITION("secondjob"),
                FORK_TRANSITION("thirdjob")),
            name = "fork")),
        DataRecord(None, Some("action"), ACTION(
            name = "secondjob",
            actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                jobu45tracker = Some("${jobTracker}"),
                nameu45node = Some("${nameNode}"),
                configuration = Some(CONFIGURATION(Seq(
                    Property2("mapred.mapper.class", "org.apache.hadoop.example.IdMapper"),
                    Property2("mapred.reducer.class", "org.apache.hadoop.example.IdReducer"),
                    Property2("mapred.map.tasks", "1"),
                    Property2("mapred.input.dir", "/usr/foo/${wf:id()}/temp1"),
                    Property2("mapred.output.dir", "/usr/foo/${wf:id()}/temp2")))))),
            ok = ACTION_TRANSITION("join"),
            error = ACTION_TRANSITION("kill"))),
        DataRecord(None, Some("action"), ACTION(
            name = "thirdjob",
            actionoption = DataRecord(None, Some("map-reduce"), MAPu45REDUCE(
                jobu45tracker = Some("${jobTracker}"),
                nameu45node = Some("${nameNode}"),
                configuration = Some(CONFIGURATION(Seq(
                    Property2("mapred.mapper.class", "org.apache.hadoop.example.IdMapper"),
                    Property2("mapred.reducer.class", "org.apache.hadoop.example.IdReducer"),
                    Property2("mapred.map.tasks", "1"),
                    Property2("mapred.input.dir", "/usr/foo/${wf:id()}/temp1"),
                    Property2("mapred.output.dir", "/usr/foo/${wf:id()}/temp3")))))),
            ok = ACTION_TRANSITION("join"),
            error = ACTION_TRANSITION("kill"))),
        DataRecord(None, Some("join"), JOIN(
            name = "join",
            to = "finaljob")),
        DataRecord(None, Some("action"), ACTION(
            name = "finaljob",
            actionoption = DataRecord(None, Some("Map-Reduce"), MAPu45REDUCE(
                jobu45tracker = Some("${jobTracker}"),
                nameu45node = Some("${nameNode}"),
                configuration = Some(CONFIGURATION(Seq(
                    Property2("mapred.mapper.class", "org.apache.hadoop.example.IdMapper"),
                    Property2("mapred.reducer.class", "org.apache.hadoop.example.IdReducer"),
                    Property2("mapred.map.tasks", "1"),
                    Property2("mapred.input.dir", "/usr/foo/${wf:id()}/temp2,usr/foo/${wf:id()}/temp3"),
                    Property2("mapred.output.dir", "${output}")))))),
            ok = ACTION_TRANSITION("end"),
            error = ACTION_TRANSITION("kill"))),
        DataRecord(None, Some("kill"), KILL(
            message = "Map/Reduce failed, error message[${wf:errorMessage()}]",
            name = "kill")))

    val wfApp = WORKFLOWu45APP(
        name = "example-forkjoinwf",
        workflowu45appoption = nodes,
        start = START("firstjob"),
        end = END("end"))
}
