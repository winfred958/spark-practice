package com.winfred.datamining.kafka

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.beans.BeanProperty

object StructStreamingTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]");
    sparkConf.set("spark.debug.maxToStringFields", "200")
    // 防止迭代次数过多, StackOverflow
    sparkConf.set("spark.executor.extraJavaOptions", "-Xss16m")

    /**
     * kyro 序列化优化
     */
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")

    val sparkSession = SparkSession
      .builder()
      .appName("KafkaVersionTest")
      .config(conf = sparkConf)
      .getOrCreate()
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "172.27.16.100:9092",
      "group.id" -> s"test-${this.getClass.getName}",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean).toString
    )

    sparkSession
      .readStream
      .format("kafka")
      .options(kafkaParams)
      .load()

    sparkSession.close()

  }


  case class LogEntity(
                        @BeanProperty server_time: Long,
                        @BeanProperty event_name: String
                      )



}
