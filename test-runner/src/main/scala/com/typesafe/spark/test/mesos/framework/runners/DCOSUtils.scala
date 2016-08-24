package com.typesafe.spark.test.mesos.framework.runners

import Utils._
import sys.process._
import com.typesafe.config.Config
import scala.language.postfixOps

object DCOSUtils {

  def waitForSparkJobDCOS(submissionId : String)(implicit config: Config): Unit = {
    var completed = false
    while (!completed) {
      val stdout = "dcos task --completed" !!

      val submissionLine = (s".*${submissionId}.*").r
      val match_ = submissionLine.findFirstIn(stdout)

      // match is found, and task is not running
      if (match_.exists(!_.contains(" R "))) {
        completed = true
      } else {
        Thread.sleep(5000)
      }
    }
  }

  def getLogOutputDCOS(taskId : String): String = {
    val cmd = s"dcos task log --completed --lines=1000 ${taskId}"
    printMsg(s"Running cmd: ${cmd}")
    cmd !!
  }

  def submitSparkJobDCOS(jarURI : String)(implicit config: Config): String = {
    val sparkJobRunnerArgs = Seq[String]("http://leader.mesos:5050",
      "dcos",
      "\"" ++ config.getString("spark.role") ++ "\"",
      config.getString("spark.attributes"),
      config.getString("spark.roleCpus")).mkString(" ")

    val submitArgs = s"--class com.typesafe.spark.test.mesos.framework.runners.SparkJobRunner " ++
      s"${jarURI} " ++ sparkJobRunnerArgs

    val cmd: Seq[String] = Seq("dcos", "--log-level=DEBUG", "spark", "--verbose", "run", s"--submit-args=${submitArgs}")

    printMsg(s"Running command: ${cmd.mkString(" ")}")
    val proc = Process(cmd, None)

    var output = ""
    proc.lineStream.foreach { line =>
      output += line + "\n"
    }
    println(output)

    val idRegex = """Submission id: (\S+)""".r
    val submissionId = idRegex.findFirstMatchIn(output).get.group(1)
    printMsg(s"Command completed.  Submission ID: ${submissionId}")

    submissionId
  }
}
