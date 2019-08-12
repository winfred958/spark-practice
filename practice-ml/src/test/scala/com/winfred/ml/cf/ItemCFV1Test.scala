package com.winfred.ml.cf

import com.winfred.ml.cf.ItemBaseCFV1.OrderItem
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object ItemCFV1Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")

    val sparkSession = SparkSession
      .builder()
      .appName("test-als")
      .config(sparkConf)
      .getOrCreate()

    val orderItemDS = getOrderItemDS(sparkSession)


    val itemMappingDS = ItemBaseCFV1
      .getItemMappingDataset(orderItemDS.toDF())

    orderItemDS.show()
    itemMappingDS.show()

    val coordinateMatrix = ItemBaseCFV1
      .createItemCoordinateMatrix(orderItemDS, itemMappingDS)


    val rows = coordinateMatrix.numRows()
    val cols = coordinateMatrix.numCols()

    println(s"=================== numRows: ${rows}, numCols: ${cols}")

    val result = ItemBaseCFV1
      .calculateSimilarities(
        sparkSession = sparkSession,
        coordinateMatrix = coordinateMatrix
      )

    result.show(200)

    sparkSession.close()
  }

  def getOrderItemDS(sparkSession: SparkSession): Dataset[OrderItem] = {

    val pathStr = Thread.currentThread().getContextClassLoader.getResource("data/order_info").toString

    import sparkSession.implicits._

    sparkSession
      .read
      .text(pathStr)
      .filter(row => {
        val str = row.getAs[String]("value")
        !StringUtils.startsWith(StringUtils.trim(str), "#")
      })
      .map(row => {
        val str = row.getAs[String]("value")
        val strings = str.split(",")

        ItemBaseCFV1
          .OrderItem(
            order_id = strings.apply(0).toLong,
            user_id = strings.apply(1),
            item_number = strings.apply(2)
          )
      })
  }


}
