package com.winfred.datamining.kafka

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.beans.BeanProperty

object SparkStreamingTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]");
    sparkConf.set("spark.debug.maxToStringFields", "200")

    val sparkSession = SparkSession
      .builder()
      .appName("KafkaVersionTest")
      .config(conf = sparkConf)
      .getOrCreate()

    sparkSession
      .readStream

    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.27.16.100:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> s"test-${this.getClass.getName}",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("kevinnhu-test")


    val result = KafkaUtils
      .createDirectStream(streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
      .map(record => {
        val str = record.value()
        JSON.parseObject(str, classOf[LogEntity])
      })
      .map(entity => {
        (entity.event_name, 1)
      })
      .reduceByKeyAndWindow((a: Int, b: Int) => {
        a + b
      }, Seconds(30), Seconds(10))


    result.print()

    sparkSession.close()
  }

  case class LogEntity(
                        @BeanProperty server_time: Long,
                        @BeanProperty event_name: String
                      )

}



