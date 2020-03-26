package com.winfred.ml.common

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, SparkSession}

object TestCase {

  def getOrderItem(
                    sparkSession: SparkSession
                  ): Dataset[OrderItem] = {
    val pathStr = Thread.currentThread().getContextClassLoader.getResource("data/order_item_test.txt").toString

    import sparkSession.implicits._
    sparkSession
      .read
      .text(pathStr)
      .map(row => {
        val value = row.getAs[String]("value")
        StringUtils.trim(value)
      })
      .filter(value => {
        StringUtils.isNotBlank(value)
      })
      .filter(value => {
        !StringUtils.startsWith(value, "#")
      })
      .map(value => {
        val strings = value.split(",")
        OrderItem(
          order_id = strings.apply(0).toLong,
          user_id = strings.apply(1),
          item_number = strings.apply(2),
          quantity = strings.apply(3).toInt
        )
      })
  }

  case class OrderItem(
                        order_id: Long = 0,
                        user_id: String = null,
                        item_number: String = null,
                        quantity: Int = 0
                      )

}
