package com.typesafe.spark.test.mesos.framework.runners

import com.typesafe.config.ConfigFactory
import Utils._;

object DCOSIntegrationTestRunner {

  def main(args: Array[String]): Unit = {

    implicit val config = ConfigFactory.load()

    val applicationJarPath = args(0)
    val result = DCOSClusterModeRunner.run(applicationJarPath)

    printMsg("TestResults:")
    println(result)

    if (result.contains("All tests passed")) {
      System.err.println("Test suite - PASS")
    } else {
      System.err.println("Test suite - FAIL")
      System.exit(1)
    }
  }

}
