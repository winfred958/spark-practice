package com.winfred.datamining.utils

import java.lang.reflect.Field
import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  *
  * @author Administrator kevin
  * @since 2017-09-12 18:37
  */
object RowUtils {

  /**
    * 根据列明匹配 String
    *
    * @param row       row
    * @param fieldName 字段名
    * @return String
    */
  def getColumnStringValue(row: Row, fieldName: String): String = {
    val value = getColumnAnyValue(row = row, fieldName = fieldName)
    if (null == value) {
      return null
    }
    StringUtils.trim(String.valueOf(value))
  }


  /**
    * 根据列明匹配 Int
    *
    * @param row       row
    * @param fieldName 字段名
    * @return Int
    */
  def getColumnIntValue(row: Row, fieldName: String): Int = {
    val tmp: AnyVal = getColumnAnyValValue(row = row, fieldName = fieldName)
    var result: Int = 0
    try {
      result = String.valueOf(tmp).toInt
    } catch {
      case e: NumberFormatException => {
        result = 0
      }
    }
    result
  }


  def getColumnLongValue(row: Row, fieldName: String): Long = {
    val tmp: AnyVal = getColumnAnyValValue(row = row, fieldName = fieldName)
    var result: Long = 0L
    try {
      result = String.valueOf(tmp).toLong
    } catch {
      case e: NumberFormatException => {
        result = 0L
      }
    }
    result
  }

  def getColumnMap(row: Row, fieldName: String): Map[_, _] = {
    val map = row.getAs[Map[_, _]](fieldName)
    map
  }

  def getColumnJavaMap(row: Row, fieldName: String): util.Map[Object, Object] = {
    val map = row.getAs[util.Map[Object, Object]](fieldName)
    map
  }


  def getColumnDoubleValue(row: Row, fieldName: String): Double = {
    val tmp: AnyVal = getColumnAnyValValue(row = row, fieldName = fieldName)
    var result: Double = 0.0
    try {
      result = String.valueOf(tmp).toDouble
    } catch {
      case e: NumberFormatException => {
        result = 0.0
      }
    }
    result
  }


  /**
    * 根据列明匹配 Any
    *
    * @param row       row
    * @param fieldName 字段名
    * @return Any
    */
  def getColumnAnyValue(row: Row, fieldName: String): Any = {
    val columns = row.schema.fieldNames
    var value: Any = null
    if (null == columns || columns.isEmpty) {
      return value
    }
    if (columns.contains(fieldName)) {
      value = row.getAs[Any](fieldName = fieldName)
    }
    value
  }


  /**
    * 根据列明匹配 AnyVal
    *
    * @param row       row
    * @param fieldName 字段名
    * @return AnyVal
    */
  def getColumnAnyValValue(row: Row, fieldName: String): AnyVal = {
    val columns = row.schema.fieldNames
    var value: AnyVal = 0
    if (null == columns || columns.isEmpty) {
      return value
    }
    if (columns.contains(fieldName)) {
      value = row.getAs[AnyVal](fieldName = fieldName)
    }
    value
  }


  /**
    * get field name list
    *
    * @param clazz
    * @return
    */
  private def getFieldNameList(clazz: Class[_]): List[String] = {
    val list: ListBuffer[String] = ListBuffer[String]()
    val fields = clazz.getDeclaredFields
    for (field <- fields) {
      list.+=(field.getName)
    }
    list.toList
  }

  /**
    * get filed hash map
    *
    * @param clazz
    */
  private def getFieldsMap(clazz: Class[_]): util.Map[String, Field] = {
    val map: util.Map[String, Field] = new util.HashMap()
    val fields = clazz.getDeclaredFields
    for (field <- fields) {
      map.put(field.getName, field);
    }
    map
  }

  /**
    * 获取参数列表
    *
    * @param row
    * @param caseClassEntity
    * @return
    */
  def copyParameters(row: Row, caseClassEntity: Object): Unit = {
    val clazz: Class[_] = caseClassEntity.getClass

    val fieldNameList = getFieldNameList(clazz)
    val fieldMap = getFieldsMap(clazz)

    fieldNameList.foreach(fieldName => {
      val field: Field = fieldMap.get(fieldName)
      val fieldType = field.getType
      val fieldTypeName = fieldType.getTypeName

      // 改变字段值
      field.setAccessible(true)

      //      StringUtils.containsAny(fieldTypeSimpleName)
      if (StringUtils.containsAny(fieldTypeName, "java.lang.String", " String")) {
        field.set(caseClassEntity, getColumnStringValue(row, fieldName))
      } else if (StringUtils.containsAny(fieldTypeName, "java.lang.Integer", "int")) {
        field.set(caseClassEntity, getColumnIntValue(row, fieldName))
      } else if (StringUtils.containsAny(fieldTypeName, "java.lang.Double", "double", "Double")) {
        field.set(caseClassEntity, getColumnDoubleValue(row, fieldName))
      } else if (StringUtils.containsAny(fieldTypeName, "java.lang.Long", "long")) {
        field.set(caseClassEntity, getColumnLongValue(row, fieldName))
      } else if (StringUtils.containsAny(fieldTypeName, "scala.collection.immutable.Map")) {
        field.set(caseClassEntity, getColumnMap(row, fieldName))
      } else if (StringUtils.containsAny(fieldTypeName, "java.util.Map")) {
        field.set(caseClassEntity, getColumnJavaMap(row, fieldName))
      } else {
        // 默认转 string
        field.set(caseClassEntity, getColumnStringValue(row, fieldName))
      }
    })
  }

  /**
    * 修改
    *
    * @param field
    * @param obj
    * @param value
    */
  private def fieldSetValue(field: Field, obj: Object, value: AnyRef): Unit = {

    // value 为 null, 不修改
    if (null == value) {
    } else {
      field.set(obj, value)
    }
    // FIXME: field 有值, 不修改
  }

  case class TestEntity(
                         order_id: Long = 1,
                         order_amount: Double = 0.0,
                         order_sn: String = "asdfasfasd",
                         map: util.Map[String, String] = new util.HashMap[String, String](),
                         scalaMap: Map[String, String] = Map()
                       )


  def main(args: Array[String]): Unit = {
    val rawValue = 10
    if (rawValue.isInstanceOf[Int]) {
      println("INT")
    }

    val testEntity: TestEntity = TestEntity()
    testEntity.getClass.getDeclaredFields.foreach(field => {
      val fieldType = field.getType
      println(s" ${fieldType.getTypeName} == ${fieldType.getSimpleName} || ${fieldType.getCanonicalName} -- ${fieldType.getName}")
    })


  }


}
