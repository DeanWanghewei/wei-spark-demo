package com.wei.spark.demo.common.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, File, FileReader, IOException}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @description: some desc
 * @author: weiyixiao
 * @email: wanghewei@kemai.cn
 * @date: 2021-12-14 17:07
 */
object SparkUtils {
  val logger = LoggerFactory.getLogger(this.getClass)
  val LOCAL_STR = "local"
  val YARN_STR = "yarn"
  val basePath = new File("./").getCanonicalPath

  private def getLocalSparkConfig(appName: String): SparkConf = {

    val file = new File(s"$basePath\\km-bigdata-ops-platform\\km-bigdata-ops-etl\\target").listFiles()

    val libPaths = file.map(_.getCanonicalPath).find(_.contains(s".jar"))

    val sparkConfMap = mutable.Map("spark.jars" -> libPaths.getOrElse(""),
      "spark.shuffle.service.enabled" -> "true",
      "spark.executor.memory" -> "9g",
      "spark.yarn.queue" -> "users.hadoop",
      "spark.driver.host" -> "192.168.241.188",
      "spark.kryoserializer.buffer.max" -> "512m",
      "spark.streaming.kafka.maxRatePerPartition" -> "100",
      "spark.yarn.jars" -> "local:/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/jars/*,local:/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/hive/*,local:/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/hbase/*,local:/usr/share/java/*",
      "spark.blacklist.enabled" -> "false",
      "spark.streaming.backpressure.initialRate" -> "100",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.master" -> "yarn",
      "spark.debug.maxToStringFields" -> "100",
      "spark.streaming.kafka.consumer.poll.ms" -> "2000",
      "spark.executor.cores" -> "8",
      "spark.cores.max" -> "40",
      "spark.executor.instances" -> "4",
      "spark.executor.memory" -> "9g",
      "spark.streaming.backpressure.enabled" -> "true",
      "spark.task.maxFailures" -> "10")
    val config = new SparkConf()
      .setMaster("yarn").setAppName(appName)
    sparkConfMap.foreach(kv => config.set(kv._1, kv._2))
    config
  }

  private def getHadoopConf(implicit cl: ClassLoader): Configuration = {
    val resBase = cl.getResource("./").getFile.replace("test-", "").dropRight(1)

    val corePath = Array("hdfs", "core", "yarn", "hive", "mapred").map(c => s"$resBase/$c-site.xml")
    val hadoopConf = new Configuration
    corePath.foreach(p => hadoopConf.addResource(new Path(p)))
    getConf(s"$resBase/spark-defaults.conf").foreach({ case (k, v) => hadoopConf.set(k, v) })
    hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    hadoopConf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle,spark_shuffle")
    hadoopConf.set("yarn.nodemanager.aux-services.spark_shuffle.class", "org.apache.spark.network.yarn.YarnShuffleService")

    val restPath = Array(s"$resBase/spark-defaults.conf")
    val coreFile = Array.concat(corePath, restPath).map(p => new File(p))
    if (coreFile.exists(f => !f.exists())) throw new Exception(s"Some config files miss, pls double check!")
    FileSystem.get(hadoopConf)
    hadoopConf
  }

  private def getConf(path: String): Map[String, String] = read(path).map(_.split("="))
    .filter(_.length == 2).map(s => (s.head, s.last)).toMap

  private def read(path: String): Array[String] = {
    var reader: BufferedReader = null
    val res = ArrayBuffer.empty[String]
    try {
      reader = new BufferedReader(new FileReader(new File(path)))
      var continue: Boolean = true
      while (continue) {
        val line = reader.readLine()
        if (line != null) res += line else continue = false
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (reader != null) {
        try {
          reader.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
    res.toArray
  }

  /**
   * 获取一个新的连接
   *
   * @param env
   * @return
   */
  def getNewSparkSession(env: String = "yarn"): SparkSession = {
    val appName = "wei-spark-etl"
    var session: SparkSession = null
    env match {
      case LOCAL_STR => {
        val sparkConf = getLocalSparkConfig(appName)
        session = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        session.sparkContext.hadoopConfiguration.addResource(getHadoopConf(this.getClass.getClassLoader))
      }
      case YARN_STR => {
        val conf = new SparkConf().setAppName(appName).setMaster("yarn").set("spark.shuffle.service.enabled", "true")
        session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
      }
      case _ => {
        logger.error(s"未适配启动的模式 ${env},已适配[${LOCAL_STR},${YARN_STR}] 运行模式")

      }
    }

    session

  }

}
