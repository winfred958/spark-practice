package com.winfred.datamining.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

/**
 * @author Administrator kevin
 */
object CommonDateUtil {

  val date_format_str_date = "yyyy-MM-dd"

  val date_format_str_datetime = "yyyy-MM-dd HH:mm:ss"

  val simpleDateFormatDateHour: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

  val date_format_str_date_month = "yyyy-MM"

  val date_format_str_date_path = "yyyy/MM/dd"

  val timeZoneLosAngeles: TimeZone = TimeZone.getTimeZone("America/Los_Angeles")

  val threadLocal_simpleDateFormatDate: ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat]()

  val threadLocal_simpleDateFormatDatePath: ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat]()

  val threadLocal_simpleDateFormatDateMonth: ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat]()

  val threadLocal_simpleDateFormatDateTime: ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat]()

  def getSimpleDateFormatDate: SimpleDateFormat = {
    var sdf: SimpleDateFormat = threadLocal_simpleDateFormatDate.get()
    if (null == sdf) {
      sdf = new SimpleDateFormat(date_format_str_date)
      sdf.setTimeZone(timeZoneLosAngeles)
      threadLocal_simpleDateFormatDate.set(sdf)
    }
    sdf
  }

  def getSimpleDateFormatDatePath: SimpleDateFormat = {
    var sdf: SimpleDateFormat = threadLocal_simpleDateFormatDatePath.get()
    if (null == sdf) {
      sdf = new SimpleDateFormat(date_format_str_date_path)
      sdf.setTimeZone(timeZoneLosAngeles)
      threadLocal_simpleDateFormatDatePath.set(sdf)
    }
    sdf
  }

  def getSimpleDateFormatMonth: SimpleDateFormat = {
    var sdf: SimpleDateFormat = threadLocal_simpleDateFormatDateMonth.get()
    if (null == sdf) {
      sdf = new SimpleDateFormat(date_format_str_date_month)
      sdf.setTimeZone(timeZoneLosAngeles)
      threadLocal_simpleDateFormatDateMonth.set(sdf)
    }
    sdf
  }

  def getSimpleDateFormatDateTime: SimpleDateFormat = {
    var sdf: SimpleDateFormat = threadLocal_simpleDateFormatDateTime.get()
    if (null == sdf) {
      sdf = new SimpleDateFormat(date_format_str_datetime)
      sdf.setTimeZone(timeZoneLosAngeles)
      threadLocal_simpleDateFormatDateTime.set(sdf)
    }
    sdf
  }

  /**
   * 获取指定后的日历
   *
   * @param day 多少天后
   * @return
   */
  def getSomeDaysLaterCalendar(day: Int): Calendar = {
    val calendar: Calendar = getCalendar()
    calendar.add(Calendar.DAY_OF_MONTH, day)
    calendar
  }

  /**
   * 获得指定days天后的日期字符串,YYYY-MM-dd
   *
   * @param day 多少天后
   */
  def getSomeDaysLaterStr(day: Int): String = {
    val calendar: Calendar = getSomeDaysLaterCalendar(day)
    calendar.setTimeZone(timeZoneLosAngeles)
    getSimpleDateFormatDate.format(calendar.getTime)
  }

  /**
   * 获得指定month月后的日期字符串,YYYY-MM
   *
   * @param month
   */
  def getSomeMonthsLaterStr(month: Int): String = {
    val calendar = getCalendar()
    calendar.add(Calendar.MONTH, month)
    calendar.setTimeZone(timeZoneLosAngeles)
    getSimpleDateFormatMonth.format(calendar.getTime)
  }

  /**
   * 获取当前时间格式, 路径使用 yyyy/MM/dd
   *
   * @return
   */
  def getFormattedDatePath(): String = {
    val calendar = getCalendar()
    calendar.setTimeZone(timeZoneLosAngeles)
    val formatDatePath = getSimpleDateFormatDatePath
    formatDatePath.setTimeZone(timeZoneLosAngeles)
    formatDatePath.format(calendar.getTime)
  }

  /**
   * yyyy-MM-dd => yyyy/MM/dd
   *
   * @param dt
   * @return
   */
  def getFormattedDatePath(dt: String): String = {
    val date = getSimpleDateFormatDate.parse(dt)
    val formatDatePath = getSimpleDateFormatDatePath
    formatDatePath.setTimeZone(timeZoneLosAngeles)
    formatDatePath.format(date)
  }

  /**
   * 获得昨天日期的年份
   *
   * @return
   */
  def getYesterdayYear(): Int = {
    val calendar = getCalendar()
    calendar.add(Calendar.DATE, -1)
    calendar.get(Calendar.YEAR)
  }

  /**
   * 获得昨天日期的月份
   *
   * @return
   */
  def getYesterdayMonth(): Int = {
    val calendar = getCalendar()
    calendar.add(Calendar.DATE, -1)
    calendar.get(Calendar.MONTH) + 1
  }

  /**
   * 获得昨天日期的Day
   *
   * @return
   */
  def getYesterdayDay(): Int = {
    val calendar = getCalendar()
    calendar.add(Calendar.DATE, -1)
    calendar.get(Calendar.DAY_OF_MONTH)
  }

  /**
   * 获取当前Calendar并指定当前服务器时区
   *
   * @return Calendar
   */
  def getCalendar(): Calendar = {
    val calendar: Calendar = Calendar.getInstance(timeZoneLosAngeles)
    calendar
  }

  /**
   * 获取指定时间戳的Calendar并指定当前服务器时区
   *
   * @return Calendar
   */
  def getCalendar(timestamp: Long): Calendar = {
    val calendar: Calendar = Calendar.getInstance(timeZoneLosAngeles)
    calendar.setTimeInMillis(timestamp)
    calendar
  }

  /**
   * 日期转化为日期字符串, 洛杉矶时间
   *
   * @param simpleDateFormat
   */
  def setTimeZoneToDateFormat(simpleDateFormat: SimpleDateFormat): Unit = {
    simpleDateFormat.setTimeZone(timeZoneLosAngeles)
  }

  /**
   * 获取当前时间 yyyy-MM-dd
   *
   * @return yyyy-MM-dd
   */
  def getCurrentDateStr(): String = {
    val dateFormat = getSimpleDateFormatDate
    dateFormat.setTimeZone(timeZoneLosAngeles)
    dateFormat.format(getCalendar().getTime)
  }


  /**
   * 获取当前时间 yyyy-MM-dd HH:mm:ss
   *
   * @return yyyy-MM-dd HH:mm:ss
   */
  def getCurrentDateTimeStr(): String = {
    val simpleDateFormatDateTime = getSimpleDateFormatDateTime
    simpleDateFormatDateTime.setTimeZone(timeZoneLosAngeles)
    simpleDateFormatDateTime.format(getCalendar().getTime)
  }

  /**
   * 时间戳转时间, yyyy-MM-dd HH:mm:ss
   *
   * @param timestamp
   * @return yyyy-MM-dd HH:mm:ss
   */
  def formatTimestamp(timestamp: Long): String = {
    val calendar: Calendar = getCalendar(timestamp)
    val simpleDateFormatDateTime = getSimpleDateFormatDateTime
    simpleDateFormatDateTime.setTimeZone(timeZoneLosAngeles)
    simpleDateFormatDateTime.format(calendar.getTime)
  }

  /**
   * 时间戳转时间, yyyy-MM-dd HH
   *
   * @param timestamp
   * @return yyyy-MM-dd HH
   */
  def formatTimestampHour(timestamp: Long): String = {
    val calendar: Calendar = getCalendar(timestamp)
    simpleDateFormatDateHour.setTimeZone(timeZoneLosAngeles)
    simpleDateFormatDateHour.format(calendar.getTime)
  }

  /**
   * 时间戳转时间, yyyy-MM-dd
   *
   * @param timestamp
   * @return yyyy-MM-dd
   */
  def formatTimestampDate(timestamp: Long): String = {
    val calendar: Calendar = getCalendar(timestamp)
    calendar.setTimeZone(timeZoneLosAngeles)
    val dateFormat = getSimpleDateFormatDate
    dateFormat.setTimeZone(timeZoneLosAngeles)
    dateFormat.format(calendar.getTime)
  }

  /**
   *
   * @param yearMonthStr yyyy-MM
   * @return
   */
  def getMonthFirstDay(yearMonthStr: String): String = {
    val date = getSimpleDateFormatMonth.parse(yearMonthStr)
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    getSimpleDateFormatDate.format(calendar.getTime)
  }

  /**
   *
   * @param yearMonthStr yyyy-MM
   * @return
   */
  def getMonthLatestDay(yearMonthStr: String): String = {
    val date = getSimpleDateFormatMonth.parse(yearMonthStr)
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    calendar.add(Calendar.MONTH, 1)
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    getSimpleDateFormatDate.format(calendar.getTime)
  }

  /**
   * 获取时间戳
   *
   * @param dateStr
   * @return
   */
  def getTimestampFromDateStr(dateStr: String): Long = {
    val simpleDateFormat = getSimpleDateFormatDate
    simpleDateFormat.setTimeZone(timeZoneLosAngeles)
    simpleDateFormat.parse(dateStr).getTime
  }


  def getYear(timestamp: Long): String = {
    val sdf = new SimpleDateFormat("yyyy")
    val calendar: Calendar = getCalendar(timestamp)
    sdf.format(calendar.getTime)
  }

  def getMonth(timestamp: Long): String = {
    val sdf = new SimpleDateFormat("MM")
    val calendar: Calendar = getCalendar(timestamp)
    sdf.format(calendar.getTime)
  }

  def getDay(timestamp: Long): String = {
    val sdf = new SimpleDateFormat("dd")
    val calendar: Calendar = getCalendar(timestamp)
    sdf.format(calendar.getTime)
  }

  def main(args: Array[String]): Unit = {
    println(CommonDateUtil.getMonthFirstDay("2019-03"))
    println(CommonDateUtil.getMonthLatestDay("2019-03"))

    println(getSimpleDateFormatDatePath)
  }
}
