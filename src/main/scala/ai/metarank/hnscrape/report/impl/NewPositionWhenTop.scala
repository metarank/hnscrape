package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder

import java.io.FileWriter
import java.time.LocalDateTime

object NewPositionWhenTop extends Report {
  case class Pos(id: Long, ts: LocalDateTime, index: Int)
  override def generate(ds: Dataset, path: String): Unit = {
    val firstAppearanceOnTop =
      ds.top.flatMap(list => list.stories.take(30).map(s => s -> list.ts)).groupBy(_._1).toList.map { case (id, list) =>
        id -> list.map(_._2).min
      }
    val newPositions = ds.news
      .flatMap(list =>
        list.stories.zipWithIndex.map { case (id, index) =>
          Pos(id, list.ts, index)
        }
      )
      .groupBy(_.id)
      .map { case (id, list) =>
        id -> list.sortBy(_.ts)
      }
    val topNewPositions = for {
      (id, topTime) <- firstAppearanceOnTop
      story         <- newPositions.get(id)
      pos           <- story.find(_.ts.isAfter(topTime)).map(_.index)
    } yield {
      pos
    }
    val csv = new CSVWriterBuilder(new FileWriter(s"$path/new_to_top_pos.csv")).withSeparator(',').build()
    topNewPositions.groupBy(identity).toList.sortBy(_._1).foreach { case (position, list) =>
      csv.writeNext(Array(position.toString, list.size.toString))
    }
    csv.close()
  }
}
