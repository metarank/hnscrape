package ai.metarank.hnscrape.report.impl

import ai.metarank.hnscrape.report.Dataset

import java.time.{DayOfWeek, LocalDateTime}
import java.time.format.DateTimeFormatter

trait Report {
  val format = DateTimeFormatter.ISO_DATE_TIME

  def startOfHour(ts: LocalDateTime): LocalDateTime =
    LocalDateTime.of(ts.getYear, ts.getMonth, ts.getDayOfMonth, ts.getHour, 0)

  def weekend(ts: LocalDateTime) = {
    (ts.getDayOfWeek == DayOfWeek.SUNDAY) || (ts.getDayOfWeek == DayOfWeek.SATURDAY)
  }

  def generate(ds: Dataset, path: String): Unit

  def main(args: Array[String]): Unit = {
    val ds = Dataset.load("/home/shutty/work/metarank/hn/")
    generate(ds, "/tmp/")
  }

}
