package ai.metarank.hnscrape.report

import ai.metarank.hnscrape.report.Dataset.{Story, StoryHistory, Top}
import com.opencsv.CSVReaderBuilder

import scala.jdk.CollectionConverters._
import java.io.FileReader
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}

case class Dataset(news: List[Top], best: List[Top], top: List[Top], show: List[Top], stories: Map[Long, Story])

object Dataset {
  case class Top(ts: Long, stories: Array[Long]) {
    val dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.from(ZoneOffset.UTC))
  }

  case class StoryHistory(by: String, id: Long, time: Long, tpe: String, title: String, hist: List[History])
  case class History(ts: Long, score: Int, comments: Int)

  case class Story(id: Long, created: Long, title: String, by: String, snapshots: List[StorySnapshot]) {
    val dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(created), ZoneId.from(ZoneOffset.UTC))
  }
  case class StorySnapshot(ts: Long, score: Int) {
    val dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.from(ZoneOffset.UTC))
  }

  case class RawStory(id: Long, created: Long, title: String, by: String, snap: Long, score: Int)

  def loadTop(file: String): List[Top] = {
    val reader = new FileReader(file)
    val csv    = new CSVReaderBuilder(reader).build()
    val records = csv.iterator().asScala.map(_.toList).collect { case head :: tail =>
      Top(head.toLong, tail.map(_.toLong).toArray)
    }
    val result = records.toList
    println(s"loaded $file")
    result
  }

  def loadStories(file: String): Map[Long, Story] = {
    val reader = new FileReader(file)
    val csv    = new CSVReaderBuilder(reader).build()
    var cnt    = 0
    val raw = csv
      .iterator()
      .asScala
      .flatMap(line =>
        for {
          title <- Option.when(line(9).nonEmpty)(line(9))
          score <- Option.when(line(7).nonEmpty)(line(7))
        } yield {
          cnt += 1
          if (cnt % 123456 == 0) println(s"loaded ${cnt} lines")
          RawStory(
            id = line(1).toLong,
            created = line(8).toLong * 1000,
            title = title,
            by = line(2),
            snap = line(0).toLong,
            score = score.toInt
          )
        }
      )
      .toList
    raw.groupMapReduce[Long, Story](_.id)(r =>
      Story(r.id, r.created, r.title, r.by, List(StorySnapshot(r.snap, r.score)))
    )((a, b) => a.copy(snapshots = a.snapshots ++ b.snapshots))
  }

  def load(dir: String) = Dataset(
    news = loadTop(s"$dir/newstories.csv"),
    best = loadTop(s"$dir/beststories.csv"),
    show = loadTop(s"$dir/showstories.csv"),
    top = loadTop(s"$dir/topstories.csv"),
    stories = loadStories(s"$dir/items.csv")
  )

}
