package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder

import java.io.FileWriter

object TopProbabilityDay extends Report {
  override def generate(ds: Dataset, path: String): Unit = {
    val topStories = ds.top.flatMap(_.stories.take(30)).toSet

    val storiesByDay = ds.stories.values.toList
      .groupBy(s => s.created.getDayOfWeek.getValue)
      .map { case (hour, stories) =>
        hour -> stories.count(s => topStories.contains(s.id)).toDouble / stories.size
      }
      .toList
      .sortBy(_._1)
    val csv = new CSVWriterBuilder(new FileWriter(s"$path/top_prob_day.csv")).withSeparator(',').build()
    storiesByDay.foreach { case (day, prob) => csv.writeNext(Array(day.toString, (100 * prob).toString)) }
    csv.close()

  }
}
