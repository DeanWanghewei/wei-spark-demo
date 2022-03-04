package com.wei.spark.demo.common.hive

import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

/**
 * @description: some desc
 * @author: weiyixiao
 * @email: wanghewei@kemai.cn
 * @date: 2021-12-14 17:43
 */
object Bootstrap {
  val logger = LoggerFactory.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {
    runLauncher(args)
  }

  def runLauncher(args: Array[String]): Unit = {
    logger.info("launcher app")
    try {

      var preCount: Option[Long] = None

      val innerThread = new Thread(new Runnable {
        override def run(): Unit = {
          val session = SparkUtils.getNewSparkSession()
          val frame = session.sql("select count(1) as cnt from default.test")
          frame.take(1).foreach((a: Row) => {
            if (a != null) {
              val cntValue = a.getAs("cnt").toString.toLong
              println("=======================================================")
              logger.info("cnt Value {}", cntValue)
              preCount = Some(cntValue)
              println(cntValue)
              println("=======================================================")
            }
          })
        }
      })

      innerThread.start()

      while (preCount.isEmpty) {
        println("================ wait for preCount value ================")
        Thread.sleep(500)
      }
      println(s"got preCount value ${preCount.get}")

    } catch {
      case exception: Exception => {
        logger.error(exception.getMessage, exception)
      }
    }
  }

}
