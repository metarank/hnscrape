package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder

import java.io.FileWriter
import java.time.LocalDateTime

object TopProbabilityHour extends Report {

  override def generate(ds: Dataset, path: String): Unit = {
    build(ds, path, dt => weekend(dt), "weekend")
    build(ds, path, dt => !weekend(dt), "workday")
  }

  def build(ds: Dataset, path: String, filter: LocalDateTime => Boolean, name: String) = {
    val topStories = ds.top.flatMap(_.stories.take(30)).toSet

    val storiesByHour = ds.stories.values.toList
      .filter(s => filter(s.created))
      .groupBy(s => s.created.getHour)
      .map { case (hour, stories) =>
        hour -> stories.count(s => topStories.contains(s.id)).toDouble / stories.size
      }
      .toList
      .sortBy(_._1)
    val csv = new CSVWriterBuilder(new FileWriter(s"$path/top_prob_$name.csv")).withSeparator(',').build()
    storiesByHour.foreach { case (hour, prob) => csv.writeNext(Array(hour.toString, (100 * prob).toString)) }
    csv.close()
  }
}
