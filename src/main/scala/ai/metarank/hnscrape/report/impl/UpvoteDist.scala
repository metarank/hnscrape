package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder

import java.io.FileWriter

object UpvoteDist extends Report {
  override def generate(ds: Dataset, path: String): Unit = {
    val csv = new CSVWriterBuilder(new FileWriter(s"$path/upvote_dist.csv")).withSeparator(',').build()
    val dist = ds.stories.values.toList
      .map(_.snapshots.map(_.score).max)
      .groupMapReduce(identity)(_ => 1)(_ + _)
      .toList
      .sortBy(_._1)
    dist.foreach { case (votes, count) =>
      csv.writeNext(Array(votes.toString, count.toString))
    }
    csv.close
  }
}
