package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder
import org.apache.commons.math3.stat.descriptive.rank.Percentile

import java.io.FileWriter

object NewTopUpvotesDistribution extends Report {
  override def generate(ds: Dataset, path: String): Unit = {
    val topStories = ds.top.flatMap(list => list.stories.map(s => list.ts -> s)).groupBy(_._2).map {
      case (id, values) => id -> values.map(_._1).min
    }

    val upvotesTop = for {
      story   <- ds.stories.values.toList
      topTime <- topStories.get(story.id)
      upvotes <- story.snapshots.find(_.ts.isAfter(topTime))
    } yield {
      upvotes.score
    }
    val distTop = upvotesTop
      .groupBy(identity)
      .map { case (upvotes, list) =>
        upvotes -> list.size
      }
      .toList
      .sortBy(_._1)
      .take(30)

    val upvotesNever = for {
      story    <- ds.stories.values.toList if !topStories.contains(story.id)
      maxScore <- story.snapshots.map(_.score).maxOption
    } yield {
      maxScore
    }

    val distNever = upvotesNever
      .groupBy(identity)
      .map { case (upvotes, list) =>
        upvotes -> list.size
      }

    val csv = new CSVWriterBuilder(new FileWriter(s"$path/upvote_top_dist.csv")).withSeparator(',').build()
    distTop.foreach { case (upvotes, count) =>
      csv.writeNext(Array(upvotes.toString, count.toString, distNever.getOrElse(upvotes, 0).toString))
    }
    csv.close()
  }
}
