package com.winfred.datamining.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale, TimeZone}

import org.apache.commons.cli.{CommandLine, Option, Options, PosixParser, UnrecognizedOptionException}
import org.apache.commons.lang3.{RegExUtils, StringUtils}

import scala.collection.mutable.ListBuffer

/**
 * ArgsHandler
 *
 * @author Administrator kevin
 * @since 2017-12-08 16:34
 */
object ArgsHandler {

  val tz = "tz"

  val dt = "dt"
  val dtf = "dtf"

  val sd = "sd"
  val sdf = "sdf"

  val ed = "ed"
  val edf = "edf"

  val defaultSimpleDateStr = "yyyy-MM-dd"

  /**
   * 针对天job 处理
   *
   * 格式: YYYY-MM-dd
   *
   * @param args
   * @return
   */
  @Deprecated
  def getArgsDateV1(args: Array[String]): ArgsBaseEntity = {
    var dateStr = if (null == args || args.isEmpty) null else args(0)
    var year: String = String.valueOf(CommonDateUtil.getSomeDaysLaterCalendar(-1).get(Calendar.YEAR))
    var month: String = String.format(Locale.ENGLISH, "%02d", new Integer(CommonDateUtil.getSomeDaysLaterCalendar(-1).get(Calendar.MONTH) + 1))
    var day: String = String.format(Locale.ENGLISH, "%02d", new Integer(CommonDateUtil.getSomeDaysLaterCalendar(-1).get(Calendar.DAY_OF_MONTH)))
    if (!StringUtils.isBlank(dateStr) && StringUtils.contains(dateStr, "-")) {
      val arr = dateStr.split("-")
      // 昨天时间yyyy-MM-dd
      year = if (arr.length > 0) arr.apply(0) else year
      month = if (arr.length > 1) arr.apply(1) else month
      day = if (arr.length > 2) arr.apply(2) else day
    } else {
      dateStr = CommonDateUtil.getSomeDaysLaterStr(-1)
    }

    return ArgsBaseEntity(
      dateStr = dateStr,
      year = year,
      month = month,
      day = day
    )
  }


  /**
   * 获取参数中时间区间 start end
   *
   * 格式: YYYY-MM-dd
   *
   * 如果end 缺省, 默认end为current昨天时间
   *
   * @return
   */
  @Deprecated
  def getArgsIntervalDateV1(args: Array[String]): ArgsIntervalDate = {
    var start: String = if (null == args || args.isEmpty) null else args(0)
    var end: String = if (null == args || args.length <= 1) null else args(1)
    var targetFormat: String = if (null == args || args.length <= 2) null else args(2)
    var targetDate: String = if (null == args || args.length <= 3) null else args(3)
    if (StringUtils.isBlank(start)) {
      start = CommonDateUtil.getSomeDaysLaterStr(-1)
    }
    if (StringUtils.isBlank(end)) {
      end = CommonDateUtil.getSomeDaysLaterStr(-1)
    }

    // 默认月报
    if (StringUtils.isBlank(targetFormat)) {
      targetFormat = "yyyy-MM"
    }
    if (StringUtils.isBlank(targetDate)) {
      val simpleDateFormatDay: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val date: Date = simpleDateFormatDay.parse(start)
      targetDate = (new SimpleDateFormat(targetFormat)).format(date)
    }

    println(s"======= start: ${start}")
    println(s"======= end: ${end}")

    return ArgsIntervalDate(
      startDateStr = start,
      endDateStr = end,
      targetDate = targetDate,
      targetFormat = targetFormat
    )
  }


  def getArgsDateV2(args: Array[String]): ArgsBaseEntity = {

    val baseDateEntity = getArgsDateEntity(args)

    val dateStr = baseDateEntity.date_str
    val dateFormat = baseDateEntity.date_format

    val timeZone = baseDateEntity.time_zone

    val calendar = Calendar.getInstance()
    calendar.setTime(dateFormat.parse(dateStr))
    calendar.setTimeZone(timeZone)

    val year: String = String.valueOf(calendar.get(Calendar.YEAR))
    val month: String = String.valueOf(calendar.get(Calendar.MONTH))
    val day: String = String.valueOf(calendar.get(Calendar.DAY_OF_MONTH))

    ArgsBaseEntity(
      dateStr = dateStr,
      year = year,
      month = month,
      day = day
    )
  }

  /**
   *
   * @param args
   * @since 2019年6月29日
   * @return
   *
   */
  def getArgsDateEntity(args: Array[String]): BaseDateEntity = {

    val options: Options = new Options()

    val dateOption = new Option(dt, "date", true, "指定的时间, 格式默认 yyyy-MM-dd")
    dateOption.setRequired(false)

    val dateFormatOption = new Option(dtf, "date-format", true, "指定的时间, 格式默认 yyyy-MM-dd")
    dateFormatOption.setRequired(false)


    val startDateOption = new Option(sd, "start-date", true, "开始时间, 格式默认 yyyy-MM-dd")
    startDateOption.setRequired(false)

    val startDateFormatOption = new Option(sdf, "start-date-format", true, "开始时间, 格式默认 yyyy-MM-dd")
    startDateFormatOption.setRequired(false)

    val endDateOption = new Option(ed, "end-date", true, "结束时间, 格式默认 yyyy-MM-dd")
    endDateOption.setRequired(false)

    val endDateFormatOption = new Option(edf, "end-date-format", true, "结束时间, 格式默认 yyyy-MM-dd")
    endDateFormatOption.setRequired(false)

    options.addOption(dateOption)
    options.addOption(dateFormatOption)
    options.addOption(startDateOption)
    options.addOption(startDateFormatOption)
    options.addOption(endDateOption)
    options.addOption(endDateFormatOption)

    var baseDateEntity = BaseDateEntity()

    val commandLine = buildCommandLine(args, options, "time-zone")

    if (commandLine.hasOption(tz)) {
      val timeZone = TimeZone.getTimeZone(commandLine.getOptionValue(tz))
      baseDateEntity = baseDateEntity.copy(time_zone = timeZone)
    }

    val theTimeZone = baseDateEntity.time_zone

    if (commandLine.hasOption(dt)) {
      baseDateEntity = baseDateEntity.copy(date_str = commandLine.getOptionValue(dt))
    }
    if (commandLine.hasOption(dtf)) {
      val str = commandLine.getOptionValue(dtf, "yyyy-MM-dd")
      val dateFormat = new SimpleDateFormat(str)
      dateFormat.setTimeZone(theTimeZone)
      baseDateEntity = baseDateEntity.copy(date_format = dateFormat)
    }

    if (commandLine.hasOption(sd)) {
      baseDateEntity = baseDateEntity.copy(start_date_str = commandLine.getOptionValue(sd))
    }
    if (commandLine.hasOption(sdf)) {
      val dateFormat = new SimpleDateFormat(commandLine.getOptionValue(sdf))
      dateFormat.setTimeZone(theTimeZone)
      baseDateEntity = baseDateEntity.copy(start_date_format = dateFormat)
    }

    if (commandLine.hasOption(ed)) {
      baseDateEntity = baseDateEntity.copy(end_date_str = commandLine.getOptionValue(ed))
    }
    if (commandLine.hasOption(edf)) {
      val dateFormat = new SimpleDateFormat(commandLine.getOptionValue(edf))
      dateFormat.setTimeZone(theTimeZone)
      baseDateEntity = baseDateEntity.copy(end_date_format = dateFormat)
    }

    baseDateEntity
  }


  /**
   * 获取指定参数
   *
   * @param args
   * @param longOpt
   * @return
   */
  def getArgsParam(args: Array[String], longOpt: String): String = {
    val options: Options = new Options()
    val commandLine = buildCommandLine(args, options, longOpt)
    commandLine.getOptionValue(longOpt)
  }

  private def buildCommandLine(args: Array[String], options: Options, opt: String): CommandLine = {
    var longOpt = opt
    if (StringUtils.startsWith(longOpt, "-")) {
      longOpt = RegExUtils.replacePattern(longOpt, "^[-]+", "")
    }
    var shotOpt = longOpt
    if (StringUtils.contains(shotOpt, "-")) {
      val listBuffer = new ListBuffer[Char]
      val array = shotOpt.toCharArray
      listBuffer.+=(array(0))
      for (i <- 0.until(array.length)) {
        if (StringUtils.equals(String.valueOf(array.apply(i)), "-")) {
          array
            .indexWhere(ch => {
              StringUtils.equals(String.valueOf(array.apply(i)), "-")
            })
          if (i + 1 < array.length) {
            listBuffer.+=(array(i + 1))
          }
        }
      }
      shotOpt = listBuffer.toArray.mkString
    }

    val option = new Option(shotOpt, longOpt, true, "")
    option.setRequired(false)
    options.addOption(option)
    try {
      val defaultParser = new PosixParser()
      val commandLine = defaultParser.parse(options, args)
      commandLine
    } catch {
      case e: UnrecognizedOptionException => {
        val o = e.getOption
        buildCommandLine(args, options, o)
      }
    }
  }


  case class ArgsBaseEntity(
                             dateStr: String,
                             year: String,
                             month: String,
                             day: String
                           )

  case class ArgsIntervalDate(
                               startDateStr: String,
                               endDateStr: String,
                               targetDate: String,
                               targetFormat: String
                             )


  case class BaseDateEntity(
                             time_zone: TimeZone = TimeZone.getTimeZone("America/Los_Angeles"),
                             date_str: String = null, // date
                             date_format: SimpleDateFormat = new SimpleDateFormat(defaultSimpleDateStr), // date_format
                             start_date_str: String = null, // start-date
                             start_date_format: SimpleDateFormat = new SimpleDateFormat(defaultSimpleDateStr), // start-date-format
                             end_date_str: String = null, // end-date
                             end_date_format: SimpleDateFormat = new SimpleDateFormat(defaultSimpleDateStr) // end-date-format
                           )

  def main(args: Array[String]): Unit = {
    val testArgs: Array[String] = Array(
      "--cycle", "60",
      "-dt", "2019-06-29",
      "--date-format", "yyyy-MM-dd",
      "--start-date", "2019-06-30",
      "--end-date", "2019-08-30",
      "--tag-label", "1d"
    )
    val entity = getArgsDateEntity(testArgs)

    val startDate = getArgsParam(testArgs, "start-date")
    println(startDate)

    val tag = getArgsParam(testArgs, "tag-label")
    println(tag)
  }

}
