package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import ai.metarank.hnscrape.report.Dataset.Top
import com.opencsv.CSVWriterBuilder

import java.io.FileWriter

object AskShowProb extends Report {
  case class StoryProb(ask: Double, show: Double, other: Double)
  override def generate(ds: Dataset, path: String): Unit = {
    val topStories = ds.top.flatMap(_.stories.take(30)).toSet

    val stories = ds.stories.values.toList
    val ask     = stories.filter(_.title.startsWith("Ask HN"))
    val show    = stories.filter(_.title.startsWith("Show HN"))
    val other   = stories.filter(s => !s.title.startsWith("Ask HN") && !s.title.startsWith("Show HN"))
    val prob = StoryProb(
      ask = ask.count(s => topStories.contains(s.id)).toDouble / ask.size,
      show = show.count(s => topStories.contains(s.id)).toDouble / show.size,
      other = other.count(s => topStories.contains(s.id)).toDouble / other.size
    )

    val csv = new CSVWriterBuilder(new FileWriter(s"$path/ask_show_prob.csv")).withSeparator(',').build()
    csv.writeNext(
      Array((100 * prob.ask).toString, (100 * prob.show).toString, (100 * prob.other).toString)
    )
    csv.close()
  }

}
