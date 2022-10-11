package ai.metarank.hnscrape.report.impl

import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder

import java.io.FileWriter

object PostActivity extends Report {
  case class DayHour(weekDay: Int, hour: Int) {
    val sortable = weekDay * 24 + hour
  }

  override def generate(ds: Dataset, path: String): Unit = {
    val csvHour = new CSVWriterBuilder(new FileWriter(s"$path/posts_hour.csv")).withSeparator(',').build()
    val hourly  = ds.stories.values.groupMapReduce(_.dt.getHour)(_ => 1)(_ + _).toList.sortBy(_._1)
    hourly.foreach { case (hour, cnt) =>
      csvHour.writeNext(Array(hour.toString, cnt.toString))
    }
    csvHour.close()

    val csvDayHour = new CSVWriterBuilder(new FileWriter(s"$path/posts_day_hour.csv")).withSeparator(',').build()
    val dayHour = ds.stories.values
      .groupMapReduce(x => DayHour(x.dt.getDayOfWeek.getValue, x.dt.getHour))(_ => 1)(_ + _)
      .toList
      .sortBy(_._1.sortable)
    dayHour.foreach { case (dh @ DayHour(day, hour), cnt) =>
      csvDayHour.writeNext(Array(day.toString, hour.toString, dh.sortable.toString, cnt.toString))
    }
    csvDayHour.close()
  }

  def main(args: Array[String]): Unit = {
    val ds = Dataset.load("/home/shutty/work/metarank/hnstat/index/")
    generate(ds, "/tmp/")
  }
}
