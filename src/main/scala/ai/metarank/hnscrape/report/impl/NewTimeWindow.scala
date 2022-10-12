package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder
import org.apache.commons.math3.stat.descriptive.rank.Percentile

import java.io.FileWriter
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

object NewTimeWindow extends Report {
  case class NewDuration(id: Long, duration: Long, ts: LocalDateTime)
  override def generate(ds: Dataset, path: String): Unit = {
    val newDuration = ds.news
      .flatMap(list => list.stories.take(30).map(id => id -> list.ts))
      .groupBy(_._1)
      .map {
        case (id, list) => {
          val min     = list.map(_._2).min
          val max     = list.map(_._2).max
          val seconds = ChronoUnit.SECONDS.between(min, max)
          NewDuration(id, seconds, list.head._2)
        }
      }
      .toList

    val csv = new CSVWriterBuilder(new FileWriter(s"$path/new_duration.csv")).withSeparator(',').build()
    newDuration.groupBy(n => n.ts.getHour).toList.sortBy(_._1).foreach { case (hour, list) =>
      val perc = new Percentile()
      perc.setData(list.map(_.duration.toDouble / 60.0).toArray)
      csv.writeNext(
        Array(hour.toString, perc.evaluate(10).toString, perc.evaluate(50).toString, perc.evaluate(90).toString)
      )
    }
    csv.close()
  }
}
