package app.utils

import java.util.Date

object Utils {
  val milliToDay : Long => Long = m => m / (1000 * 3600 * 24)

  def getKey(date : Date) : Long = {
    val timestamp = date.getTime
    milliToDay(timestamp)
  }
}
