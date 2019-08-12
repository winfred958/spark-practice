package com.winfred.ml.cf

import com.winfred.datamining.utils.{ArgsHandler, CommonDateUtil, RowUtils}
import com.winfred.ml.common.DataMappingUtils
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * Order
 *
 * ItemBaseCF v1
 *
 * 1. 商品组合构造 IndexedRow
 * 2. 构造商品矩阵: IndexedRow -> IndexedRowMatrix
 * 3. 计算商品相似度
 * 4. item 权重调整, 惩罚爆款
 * 5. sink
 *
 * @author kevin
 * @since 2019年8月9日10:10:02
 */
object ItemBaseCFV1 {

  val max_per_item = 30

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("yarn");
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
      .appName("ItemBaseCFV1")
      .config(conf = sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val baseDateEntity = ArgsHandler.getArgsDateEntity(args = args)
    val startDateStr = baseDateEntity.start_date_str

//    //
//    val orderItemDataset = getOrderItemDataset(
//      sparkSession = sparkSession,
//      startDateStr = startDateStr
//    )
//
//    //item mapping
//    val itemMapping: Dataset[ItemMapping] = getItemMappingDataset(
//      orderItemDataset.toDF()
//    )
//
//    // 构造 item 矩阵
//    val coordinateMatrix: CoordinateMatrix = createItemCoordinateMatrix(orderItemDataset, itemMapping)
//
//    // 计算相似度, 转换 dataset
//    val similarities: Dataset[TmpEntity] = calculateSimilarities(
//      sparkSession = sparkSession,
//      coordinateMatrix = coordinateMatrix
//    )
//
//    // 每个商品取top30
//    val needMappingResult: Dataset[TmpEntity] = getSimilaritiesTop30(similarities, max_per_item)
//
//    // item recovery
//    val resultDS = itemRecovery(needMappingResult, itemMapping)
//
//    // sink
//    //    sinkToMysql(resultDS = resultDS)
//
//    sparkSession.close()
  }


  /**
   * item mapping 还原
   *
   * @param itemMapping
   * @param needMappingResult
   * @return
   */
  private def itemRecovery(needMappingResult: Dataset[TmpEntity], itemMapping: Dataset[ItemMapping]) = {
    val sparkSession = needMappingResult.sparkSession

    import sparkSession.implicits._

    val resultDS: Dataset[ItemBaseCFV1Entity] = needMappingResult.as("source")
      .join(itemMapping.as("mapping1"), $"source.item_mapping_id_1" === $"mapping1.item_mapping_id", "left")
      .join(itemMapping.as("mapping2"), $"source.item_mapping_id_2" === $"mapping2.item_mapping_id", "left")
      .selectExpr(
        "mapping1.item_number AS item_number_1",
        "mapping2.item_number AS item_number_2",
        "source.similar"
      ).map(row => {
      val entity = ItemBaseCFV1Entity()
      RowUtils.copyParameters(row, entity)
      entity.copy(
        update_time = CommonDateUtil.getCurrentDateTimeStr(),
        update_timestamp = System.currentTimeMillis()
      )
    })
    resultDS
  }

  //  /**
  //    * sink to mysql
  //    *
  //    * @param resultDS
  //    */
  //  def sinkToMysql(resultDS: Dataset[ItemBaseCFV1Entity]): Unit = {
  //    resultDS
  //      .foreachPartition(it => {
  //        val recommendSinkSqlSessionFactory = new RecommendSinkSqlSessionFactory(CollaborativeFilteringDao.DATASOURCE_NAME)
  //        val sqlSessionFactory = recommendSinkSqlSessionFactory.getSqlSessionFactory
  //        var sqlSession: SqlSession = null
  //        it
  //          .map(e => {
  //            val itemBaseCFV1Entity: CfItemBaseCFV1Entity = new CfItemBaseCFV1Entity()
  //            ReflectUtils.copyProperties(e, itemBaseCFV1Entity)
  //            itemBaseCFV1Entity
  //          })
  //          .foreach(e => {
  //            try {
  //              sqlSession = sqlSessionFactory.openSession(true)
  //              val collaborativeFilteringDao = sqlSession.getMapper(classOf[CollaborativeFilteringDao])
  //              collaborativeFilteringDao.replaceItemBaseCFV1(e)
  //            } catch {
  //              case e: ClassNotFoundException => {
  //                throw e
  //              }
  //              case e: InstantiationException => {
  //                throw e
  //              }
  //              case e: IllegalAccessException => {
  //                throw e
  //              }
  //            }
  //            finally {
  //              if (null != sqlSession) {
  //                sqlSession.close();
  //              }
  //            }
  //          })
  //      })
  //
  //  }
  //
  //  /**
  //    * 获取订单数据集
  //    *
  //    * @param sparkSession
  //    * @param startDateStr
  //    * @return
  //    */
  //  def getOrderItemDataset(
  //                           sparkSession: SparkSession,
  //                           startDateStr: String
  //                         ): Dataset[OrderItem] = {
  //
  //    val orderItemBaseDS = OrderInfoCore.getOrderItemDatasetWithSeller(sparkSession = sparkSession, startDateStr = startDateStr)
  //
  //    orderItemBaseDS
  //      .map(entity => {
  //        OrderItem(
  //          order_id = entity.order_id,
  //          user_id = String.valueOf(entity.user_id),
  //          item_number = entity.item_number
  //
  //        )
  //      })
  //  }

  /**
   * item number mapping,
   *
   * @param df
   * @return
   */
  def getItemMappingDataset(df: DataFrame): Dataset[ItemMapping] = {
    val sparkSession = df.sparkSession

    import sparkSession.implicits._

    DataMappingUtils
      .getMappingDF(
        df,
        "item_number",
        "item_mapping_id"
      )
      .map(row => {
        val itemMapping = ItemMapping()
        RowUtils.copyParameters(row, itemMapping)
        itemMapping
      })
  }

  /**
   * 获取商品组合
   *
   * @param itemArray
   * @return
   */
  def getItemCombineList(itemArray: Array[OrderItemMapping]): ListBuffer[(Long, Long)] = {
    val list = new ListBuffer[(Long, Long)]
    val size = itemArray.size
    if (size < 2) {
      list.+=((-1, -1))
      return list
    }

    for (i <- 0.until(size - 1)) {
      for (j <- (i + 1).until(size)) {
        list.+=((itemArray.apply(i).item_mapping_id, itemArray.apply(j).item_mapping_id))
      }
    }
    list
  }

  /**
   * 构建坐标矩阵
   *
   * @param orderItemDataset
   * @param itemMapping
   * @return
   */
  def createItemCoordinateMatrix(
                                  orderItemDataset: Dataset[OrderItem],
                                  itemMapping: Dataset[ItemMapping]
                                ): CoordinateMatrix = {
    val sparkSession = orderItemDataset.sparkSession

    import sparkSession.implicits._

    val orderItemMappingDataset: Dataset[OrderItemMapping] = orderItemDataset
      .join(itemMapping, Seq("item_number"), "inner")
      .map(row => {
        val orderItemMapping = OrderItemMapping()
        RowUtils.copyParameters(row, orderItemMapping)
        orderItemMapping
      })

    val entries: RDD[MatrixEntry] = orderItemMappingDataset
      .rdd
      .map(entity => {
        (entity.user_id, entity)
      })
      .groupByKey()
      .map(x => {
        val userId = x._1
        val itemArray = x._2.toArray
        val list = getItemCombineList(itemArray = itemArray)
        for (e <- list) yield e
      })
      .flatMap(x => x)
      .filter(x => {
        x._1 >= 0 && x._2 >= 0
      })
      .map(x => {
        ((x._1, x._2), 1L)
      })
      .reduceByKey((a, b) => {
        a + b
      })
      .map(x => {
        new MatrixEntry(x._1._1, x._1._2, x._2)
      })

    new CoordinateMatrix(entries = entries)
  }

  //  /**
  //    * 构造商品矩阵
  //    *
  //    * @param orderItemDataset
  //    * @return
  //    */
  //  def createItemIndexedRowMatrix(
  //                                  orderItemDataset: Dataset[OrderItem],
  //                                  itemMapping: Dataset[ItemMapping]
  //                                ): IndexedRowMatrix = {
  //
  //    val sparkSession = orderItemDataset.sparkSession
  //
  //    import sparkSession.implicits._
  //
  //
  //    val itemSize: Int = itemMapping
  //      .groupBy()
  //      .count()
  //      .collectAsList()
  //      .get(0)
  //      .getAs[Long]("count")
  //      .toInt
  //
  //    print(s"============== item size : ${itemSize}")
  //
  //
  //    val orderItemMappingDataset: Dataset[OrderItemMapping] = orderItemDataset
  //      .join(itemMapping, Seq("item_number"), "inner")
  //      .map(row => {
  //        val orderItemMapping = OrderItemMapping()
  //        RowUtils.copyParameters(row, orderItemMapping)
  //        orderItemMapping
  //      })
  //
  //    val indexedRowRDD = orderItemMappingDataset
  //      .rdd
  //      .map(entity => {
  //        (entity.user_id, entity)
  //      })
  //      .groupByKey()
  //      .map(x => {
  //        val userId = x._1
  //
  //        val list = new ListBuffer[(Long, Long)]
  //
  //        val itemArray = x._2.toArray
  //
  //        val size = itemArray.size
  //        if (size < 2) {
  //          list.+=((-1, -1))
  //        } else {
  //          for (i <- 0.until(size - 1)) {
  //            val itemA = itemArray.apply(i).item_mapping_id
  //            val itemB = itemArray.apply(i + 1).item_mapping_id
  //            // FIXME: itemA, itemB 顺序
  //            list.+=((itemA, itemB))
  //          }
  //        }
  //        for (e <- list) yield e
  //      })
  //      .flatMap(x => x)
  //      .filter(x => {
  //        x._1 >= 0 && x._2 >= 0
  //      })
  //      .map(x => {
  //        ((x._1, x._2), 1L)
  //      })
  //      .reduceByKey((a, b) => {
  //        a + b
  //      })
  //      .map(x => {
  //        val itemCombination = x._1
  //        (itemCombination._1, (x))
  //      })
  //      .groupByKey()
  //      .map(x => {
  //        val indexList = new ListBuffer[Long]
  //        val valueList = new ListBuffer[Double]
  //
  //        val itemA = x._1
  //        x._2.foreach(t => {
  //          val itemB = t._1._2
  //          val count = t._2
  //
  //          indexList.+=(itemB)
  //          valueList.+=(count)
  //        })
  //
  //        IndexedRow(itemA, Vectors.sparse(itemSize, indexList.toArray, valueList.toArray))
  //      })
  //
  //
  //    val indexedRowMatrix: IndexedRowMatrix = new IndexedRowMatrix(indexedRowRDD)
  //    indexedRowMatrix
  //  }


  /**
   * 计算相似度
   *
   * @param sparkSession
   * @param coordinateMatrix
   * @return
   */
  def calculateSimilarities(
                             sparkSession: SparkSession,
                             coordinateMatrix: CoordinateMatrix
                           ): Dataset[TmpEntity] = {
    import sparkSession.implicits._

    coordinateMatrix
      .toIndexedRowMatrix()
      .columnSimilarities()
      .transpose()
      .toIndexedRowMatrix()
      .rows
      .map(x => {
        val list = new ListBuffer[TmpEntity]

        //eg. (index = 587, Vector(1225,[612,613],[1.0,1.0]))

        val item_mapping_id_1 = x.index
        val vector = x.vector

        vector.foreachActive((index, value) => {
          list.+=(
            TmpEntity(
              item_mapping_id_1 = item_mapping_id_1,
              item_mapping_id_2 = index,
              similar = value
            )
          )
        })

        for (e <- list) yield e
      })
      .flatMap(x => x)
      .toDS()
  }


  /**
   * 获取 top30
   *
   * @param similarities
   */
  def getSimilaritiesTop30(
                            similarities: Dataset[TmpEntity],
                            topN: Int
                          ): Dataset[TmpEntity] = {

    val sparkSession = similarities.sparkSession

    import sparkSession.implicits._

    similarities
      .selectExpr(
        "item_mapping_id_1",
        "item_mapping_id_2",
        "similar",
        "RANK() OVER(PARTITION BY item_mapping_id_1 ORDER BY similar DESC) AS rank"
      )
      .where(s"rank <= ${topN}")
      .map(row => {
        val entity = TmpEntity()
        RowUtils.copyParameters(row, entity)
        entity
      })
  }

  case class TmpEntity(
                        item_mapping_id_1: Long = 0L,
                        item_mapping_id_2: Long = 0L,
                        similar: Double = 0.0
                      )

  case class OrderItemMapping(
                               order_id: Long = 0L,
                               user_id: String = "",
                               item_number: String = "",
                               item_mapping_id: Long = 0L
                             )

  case class OrderItem(
                        order_id: Long,
                        user_id: String,
                        item_number: String
                      )

  case class ItemMapping(
                          item_number: String = "",
                          item_mapping_id: Long = 0
                        )

  case class ItemBaseCFV1Entity(
                                 item_number_1: String = "",
                                 item_number_2: String = "",
                                 similar: Double = 0.0,
                                 update_time: String = "",
                                 update_timestamp: Long = 0L
                               )

}
