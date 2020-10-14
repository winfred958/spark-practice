package com.winfred.datamining.kafka

import com.alibaba.fastjson.JSON
import com.winfred.datamining.utils.ArgsHandler
import com.winfred.entity.EventEntity
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.beans.BeanProperty

object SparkStreamingTest {

  val log: Logger = LoggerFactory.getLogger("SparkStreamingTest")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.debug.maxToStringFields", "200")

    val bootstrapServers = ArgsHandler.getArgsParam(args, "bootstrap-servers")
    val topicName = ArgsHandler.getArgsParam(args, "topic-name")

    log.warn("[kafka] servers: {}", bootstrapServers)
    log.warn("[kafka] servers: {}", topicName)

    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> s"test-${this.getClass.getName}",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean)
    )

    val topics = Array(topicName)

    val result = KafkaUtils
      .createDirectStream(streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
      .map(record => {
        val str = record.value()
        JSON.parseObject(str, classOf[EventEntity])
      })
      .map(entity => {
        ("count", 1)
      })
      .reduceByKeyAndWindow((a: Int, b: Int) => {
        a + b
      }, Seconds(30), Seconds(10))


    result.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  case class LogEntity(
                        @BeanProperty server_time: Long = System.currentTimeMillis(),
                        @BeanProperty event_name: String = "test"
                      )

}



