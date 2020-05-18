package com.winfred.datamining.kafka

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.junit.{Before, Test}

import scala.beans.BeanProperty

class JsonTest {
  private var jsonStr: String = null

  @Before
  def buildTestData(): Unit = {
    val entity = TestEntity(
      server_time = System.currentTimeMillis(),
      event_name = "test"
    )
    jsonStr = JSON.toJSONString(entity, 1, SerializerFeature.SortField)
  }

  @Test
  def toJson(): Unit = {
    val entity = TestEntity(
      server_time = System.currentTimeMillis(),
      event_name = "test"
    )
    println(JSON.toJSONString(entity, 1, SerializerFeature.SortField))
  }

  @Test
  def toObject(): Unit = {
    val entity1 = JSON.parseObject(jsonStr, classOf[TestEntity])

    println(entity1.event_name)
  }

}

case class TestEntity(
                       @BeanProperty server_time: Long = System.currentTimeMillis(),
                       @BeanProperty event_name: String = "test"
                     )
