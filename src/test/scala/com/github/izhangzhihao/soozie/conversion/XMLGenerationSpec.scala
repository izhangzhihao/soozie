package com.github.izhangzhihao.soozie.conversion

import com.github.izhangzhihao.soozie.dsl._
import com.github.izhangzhihao.soozie.jobs.{JavaJob, MapReduceJob}
import com.github.izhangzhihao.soozie.writer.implicits._
import oozie._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.specs2.mutable._
import oozie._
import scalaxb._

class XMLGenerationSpec extends Specification {
    "XML Generation" should {
        "be able to successfully generate a bundle" in {
            val bundle = Bundle(
                name = "APPNAME",
                parameters = List("appPath" -> None, "appPath2" -> Some("hdfs://foo:9000/user/joe/job/job.properties")),
                kickoffTime = Right("${kickOffTime}"),
                coordinators = List(
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle1",
                        path = Some("${appPath}"),
                        coordinator = coordinator("coordJobFromBundle1"),
                        configuration = List("startTime1" -> "${START_TIME}", "endTime1" -> "${END_TIME}")
                    ),
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle2",
                        path = Some("${appPath}"),
                        coordinator = coordinator("coordJobFromBundle2"),
                        configuration = List("startTime2" -> "${START_TIME2}", "endTime2" -> "${END_TIME2}")
                    )
                )
            )

            val expectedResult = """<bundle-app name="APPNAME" xmlns="uri:oozie:bundle:0.2">
                                   |    <parameters>
                                   |        <property>
                                   |            <name>appPath</name>
                                   |        </property>
                                   |        <property>
                                   |            <name>appPath2</name>
                                   |            <value>hdfs://foo:9000/user/joe/job/job.properties</value>
                                   |        </property>
                                   |    </parameters>
                                   |    <controls>
                                   |        <kick-off-time>${kickOffTime}</kick-off-time>
                                   |    </controls>
                                   |    <coordinator name="coordJobFromBundle1">
                                   |        <app-path>${appPath}</app-path>
                                   |        <configuration>
                                   |            <property>
                                   |                <name>startTime1</name>
                                   |                <value>${START_TIME}</value>
                                   |            </property>
                                   |            <property>
                                   |                <name>endTime1</name>
                                   |                <value>${END_TIME}</value>
                                   |            </property>
                                   |        </configuration>
                                   |    </coordinator>
                                   |    <coordinator name="coordJobFromBundle2">
                                   |        <app-path>${appPath}</app-path>
                                   |        <configuration>
                                   |            <property>
                                   |                <name>startTime2</name>
                                   |                <value>${START_TIME2}</value>
                                   |            </property>
                                   |            <property>
                                   |                <name>endTime2</name>
                                   |                <value>${END_TIME2}</value>
                                   |            </property>
                                   |        </configuration>
                                   |    </coordinator>
                                   |</bundle-app>""".stripMargin

            bundle.toXml() must_== expectedResult
        }

        "be able to successfully generate a coordinator" in {
            import oozie.coordinator_0_4._

            val timezone = DateTimeZone.forID("America/Los_Angeles")

            val coordinator = Coordinator(
                parameters = Nil,
                controls = Some(CONTROLS(Some(CONTROLSSequence1(
                    timeout = Some("10"),
                    concurrency = Some("${concurrency_level}"),
                    execution = Some("${execution_order}"),
                    throttle = Some("${materialization_throttle}")
                )))),
                datasets = Some(DATASETS(Some(DATASETSSequence1(
                    datasetsoption = Seq(
                        DataRecord(None, Some("dataset"), SYNCDATASET(
                            name = "din",
                            frequency = "${coord:endOfDays(1)}",
                            initialu45instance = "2009-01-02T08:00Z",
                            timezone = "America/Los_Angeles",
                            uriu45template = "${baseFsURI}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}",
                            doneu45flag = None)),
                        DataRecord(None, Some("dataset"), SYNCDATASET(
                            name = "dout",
                            frequency = "${coord:minutes(30)}",
                            initialu45instance = "2009-01-02T08:00Z",
                            timezone = "UTC",
                            uriu45template = "${baseFsURI}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}",
                            doneu45flag = None))
                    ))))),
                inputEvents = Some(INPUTEVENTS(Seq(
                    DATAIN(
                        name = "input",
                        dataset = "din",
                        datainoption = Seq(DataRecord(None, Some("instance"), "${coord:current(0)}"))
                    )))),
                outputEvents = Some(OUTPUTEVENTS(Seq(DATAOUT(
                    name = "output",
                    dataset = "dout",
                    instance = "${coord:current(1)}"
                )))),
                workflowPath = Some("${wf_app_path}"),
                configuration = List(
                    "wfInput" -> "${coord:dataIn('input')}",
                    "wfOutput" -> "${coord:dataOut('output')}"),
                frequency = Days(1),
                start = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm").withZone(timezone).parseDateTime("2009-01-02T00:00"),
                end = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm").withZone(timezone).parseDateTime("2009-01-04T00:00"),
                timezone = timezone,
                name = "hello-coord",
                workflow = workflow
            )

            val expectedResult = """<coordinator-app timezone="America/Los_Angeles" end="2009-01-04T08:00Z" start="2009-01-02T08:00Z" frequency="${coord:days(1)}" name="hello-coord" xmlns="uri:oozie:coordinator:0.4">
                                   |    <controls>
                                   |        <timeout>10</timeout>
                                   |        <concurrency>${concurrency_level}</concurrency>
                                   |        <execution>${execution_order}</execution>
                                   |        <throttle>${materialization_throttle}</throttle>
                                   |    </controls>
                                   |    <datasets>
                                   |        <dataset timezone="America/Los_Angeles" initial-instance="2009-01-02T08:00Z" frequency="${coord:endOfDays(1)}" name="din">
                                   |            <uri-template>${baseFsURI}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}</uri-template>
                                   |        </dataset>
                                   |        <dataset timezone="UTC" initial-instance="2009-01-02T08:00Z" frequency="${coord:minutes(30)}" name="dout">
                                   |            <uri-template>${baseFsURI}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}</uri-template>
                                   |        </dataset>
                                   |    </datasets>
                                   |    <input-events>
                                   |        <data-in dataset="din" name="input">
                                   |            <instance>${coord:current(0)}</instance>
                                   |        </data-in>
                                   |    </input-events>
                                   |    <output-events>
                                   |        <data-out dataset="dout" name="output">
                                   |            <instance>${coord:current(1)}</instance>
                                   |        </data-out>
                                   |    </output-events>
                                   |    <action>
                                   |        <workflow>
                                   |            <app-path>${wf_app_path}</app-path>
                                   |            <configuration>
                                   |                <property>
                                   |                    <name>wfInput</name>
                                   |                    <value>${coord:dataIn('input')}</value>
                                   |                </property>
                                   |                <property>
                                   |                    <name>wfOutput</name>
                                   |                    <value>${coord:dataOut('output')}</value>
                                   |                </property>
                                   |            </configuration>
                                   |        </workflow>
                                   |    </action>
                                   |</coordinator-app>""".stripMargin

            coordinator.toXml() must_== expectedResult
        }

        "given a user created job it should generate the correct workflow" in {
            import oozie.shell_0_3._

            case class MyShell(jobName: String = "shell-test") extends Job[ACTION] {
                override val record: DataRecord[ACTION] = DataRecord(None, Some("shell"), ACTION(
                    jobu45tracker = None,
                    nameu45node = None,
                    prepare = None,
                    jobu45xml = Nil,
                    configuration = None,
                    exec = "echo test",
                    argument = Nil,
                    envu45var = Nil,
                    file = Nil,
                    archive = Nil,
                    captureu45output = None,
                    xmlns = "uri:oozie:shell-action:0.3"))
            }

            val firstJob = MyShell("test") dependsOn Start
            val end = End dependsOn firstJob
            val workflow = Workflow("test-user-action", end)

            val expectedResult =
                """<workflow-app name="test-user-action" xmlns="uri:oozie:workflow:0.5">
                  |    <start to="test"/>
                  |    <action name="test">
                  |        <shell xmlns="uri:oozie:shell-action:0.3">
                  |            <exec>echo test</exec>
                  |        </shell>
                  |        <ok to="end"/>
                  |        <error to="kill"/>
                  |    </action>
                  |    <kill name="kill">
                  |        <message>test-user-action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
                  |    </kill>
                  |    <end name="end"/>
                  |</workflow-app>""".stripMargin

            workflow.toXml() must_== expectedResult
        }

        "give workflow with double quotes rather than &quot;" in {
            val firstJob = MapReduceJob("mr_first", Some("${nameNode}"), Some("${jobTracker}")) dependsOn Start
            val jsonJob = JavaJob(
                jobName = "java_class",
                mainClass = "test.class",
                configuration = List(
                    "testJson" -> """{ "foo" : "bar" }"""
                ),
                nameNode = Some("${nameNode}"),
                jobTracker = Some("${jobTracker}")
            ) dependsOn firstJob
            val end = End dependsOn jsonJob
            val workflow = Workflow("test-post-processing", end)

            workflow.toXml() must_== postProcessedXml
        }
    }

    val workflow = {
        val firstJob = MapReduceJob("mr_first") dependsOn Start
        val jsonJob = JavaJob(
            jobName = "java_class",
            mainClass = "test.class",
            configuration = List(
                "testJson" -> """{ "foo" : "bar" }"""
            ),
            nameNode = Some("${nameNode}"),
            jobTracker = Some("${jobTracker}")
        ) dependsOn firstJob
        val end = End dependsOn jsonJob

        Workflow("test-post-processing", end)
    }

    def coordinator(name: String) = {
        Coordinator(
            name = name,
            workflow = workflow,
            timezone = DateTimeZone.forID("Australia/Sydney"),
            start = DateTime.now(),
            end = DateTime.now().plusDays(10),
            frequency = Days(24),
            configuration = Nil,
            workflowPath = Some("/fake/path")
        )
    }

    val postProcessedXml =
        """<workflow-app name="test-post-processing" xmlns="uri:oozie:workflow:0.5">
    <start to="mr_first"/>
    <action name="mr_first">
        <map-reduce>
            <job-tracker>${jobTracker}</job-tracker>
            <name-node>${nameNode}</name-node>
        </map-reduce>
        <ok to="java_class"/>
        <error to="kill"/>
    </action>
    <action name="java_class">
        <java>
            <job-tracker>${jobTracker}</job-tracker>
            <name-node>${nameNode}</name-node>
            <configuration>
                <property>
                    <name>testJson</name>
                    <value>{ "foo" : "bar" }</value>
                </property>
            </configuration>
            <main-class>test.class</main-class>
        </java>
        <ok to="end"/>
        <error to="kill"/>
    </action>
    <kill name="kill">
        <message>test-post-processing failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>"""
}