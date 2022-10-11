package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder
import org.apache.commons.math3.stat.descriptive.rank.Percentile

import java.io.FileWriter
import java.time.{DayOfWeek, LocalDateTime, ZoneOffset}

object PostsWeekend extends Report {
  override def generate(ds: Dataset, path: String): Unit = {
    build(ds, path, ts => weekend(ts), "hour_weekend")
    build(ds, path, ts => !weekend(ts), "hour_weekday")
    build(ds, path, ts => true, "hour_everyday")
  }

  def build(ds: Dataset, path: String, filter: LocalDateTime => Boolean, name: String) = {
    val csvday = new CSVWriterBuilder(new FileWriter(s"$path/posts_$name.csv")).withSeparator(',').build()
    val day = ds.stories.values
      .groupMapReduce(s => startOfHour(s.created))(_ => 1)(_ + _)
      .toList
      .filter(x => filter(x._1))
      .groupBy(k => k._1.getHour)
      .map(k => k._1 -> k._2.map(_._2))
      .toList
      .sortBy(_._1)
    day.foreach { case (hour, cnt) =>
      val avg = cnt.sum.toDouble / cnt.size
      val p   = new Percentile()
      p.setData(cnt.map(_.toDouble).toArray)
      val median = p.evaluate(50)
      val p90    = p.evaluate(90)
      val p10    = p.evaluate(10)
      csvday.writeNext(Array(hour.toString, avg.toString, median.toString, p90.toString, p10.toString))
    }
    csvday.close()
  }
}
