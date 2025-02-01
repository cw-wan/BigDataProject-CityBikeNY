package common

import java.text.SimpleDateFormat
import java.util.Date

object Utils {

  /**
   * parse date string as date object
   *
   * @param dateStr date string
   * @param format  date format, Constants.DATE_FORMAT by default
   * @return parsed date object
   */
  def parseDate(dateStr: String, format: String = Constants.DATE_FORMAT): Date = {
    val formatter = new SimpleDateFormat(format)
    formatter.parse(dateStr)
  }

  /**
   * print log
   *
   * @param message log message
   */
  def log(message: String): Unit = {
    println(s"[${formatDate(new Date())}] INFO: $message")
  }

  /**
   * Format date object as string
   *
   * @param date   date object
   * @param format date format, Constants.DATE_FORMAT by default
   * @return date string
   */
  def formatDate(date: Date, format: String = Constants.DATE_FORMAT): String = {
    val formatter = new SimpleDateFormat(format)
    formatter.format(date)
  }
}
