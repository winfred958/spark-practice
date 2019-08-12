package com.winfred.ml.common

import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row}

object DataMappingUtils {
  /**
    * DataFrame + item_mapping_id
    *
    * @param dataFrame 必须包含 item_number 字段
    * @return
    */
  def addItemNumberMappingId(dataFrame: DataFrame): DataFrame = {
    addMappingCol(dataFrame = dataFrame, "item_number", "item_mapping_id")
  }

  /**
    * DataFrame + user_mapping_id
    *
    * @param dataFrame 必须包含 item_number 字段
    * @return
    */
  def addUserMappingId(dataFrame: DataFrame): DataFrame = {
    addMappingCol(dataFrame = dataFrame, "user_id", "user_mapping_id")
  }

  /**
    * dataFrame 根据 needMappingCol 添加 mappingCol
    *
    * @param dataFrame
    * @param needMappingCol
    * @param mappingCol
    * @return + mappingCol
    */
  def addMappingCol(dataFrame: DataFrame, needMappingCol: String, mappingCol: String): DataFrame = {
    val mappingDF = getMappingDF(dataFrame = dataFrame, needMappingCol, mappingCol)
    dataFrame
      .join(mappingDF, Seq(needMappingCol), "inner")
  }

  /**
    * get user mapping
    *
    * + user_mapping_id
    *
    * @param dataFrame      必须包含 user_id 字段
    * @param needMappingCol 需要mapping 列
    * @param mappingCol     mapping 列
    * @return DataSet[Row[needMappingCol, mappingCol]]
    **/
  def getMappingDF(dataFrame: DataFrame, needMappingCol: String, mappingCol: String): DataFrame = {
    val sparkSession = dataFrame.sparkSession

    val sourceDF = dataFrame
      .select(needMappingCol)
      .distinct()
      .orderBy(s"${needMappingCol}")

    val addMappingRdd = sourceDF
      .rdd
      .zipWithIndex()
      .map(x => {
        Row.merge(x._1, Row(x._2))
      })

    val addMappingSchema = sourceDF.schema.add(mappingCol, LongType)

    sparkSession
      .createDataFrame(
        addMappingRdd,
        addMappingSchema
      )
  }
}
