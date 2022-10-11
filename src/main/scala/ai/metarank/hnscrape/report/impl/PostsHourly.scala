package ai.metarank.hnscrape.report.impl

import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder

import java.io.FileWriter
import java.time.{DayOfWeek, LocalDateTime, ZoneOffset}

object PostsHourly extends Report {


  override def generate(ds: Dataset, path: String): Unit = {
    val csvHour = new CSVWriterBuilder(new FileWriter(s"$path/posts_hourly.csv")).withSeparator(',').build()

    val hourly = ds.stories.values
      .groupMapReduce(s => startOfHour(s.created))(_ => 1)(_ + _)
      .toList
      .sortBy(_._1.toInstant(ZoneOffset.UTC))
    hourly.foreach { case (hour, cnt) =>
      csvHour.writeNext(Array(format.format(hour), cnt.toString))
    }
    csvHour.close()

    val csvday = new CSVWriterBuilder(new FileWriter(s"$path/posts_weekend.csv")).withSeparator(',').build()
    val day = ds.stories.values
      .groupMapReduce(s => startOfHour(s.created))(_ => 1)(_ + _)
      .toList
      .filter(x => weekend(x._1))
      .groupBy(k => k._1.getHour)
      .map(k => k._1 -> k._2.map(_._2))
      .map { case (hour, counts) =>
        hour -> counts.sum / counts.size.toDouble
      }
      .toList
      .sortBy(_._1)
    day.foreach { case (hour, cnt) =>
      csvday.writeNext(Array(hour.toString, cnt.toString))
    }
    csvday.close()
  }

}
