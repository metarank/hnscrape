package ai.metarank.hnscrape.report

import ai.metarank.hnscrape.report.Dataset.{Story, StoryHistory, Top}
import com.opencsv.CSVReaderBuilder

import scala.jdk.CollectionConverters._
import java.io.FileReader
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}

case class Dataset(news: List[Top], best: List[Top], top: List[Top], show: List[Top], stories: Map[Long, Story])

object Dataset {
  case class Top(ts: LocalDateTime, stories: Array[Long]) {}

  case class StoryHistory(by: String, id: Long, time: LocalDateTime, tpe: String, title: String, hist: List[History])
  case class History(ts: Long, score: Int, comments: Int)

  case class Story(id: Long, created: LocalDateTime, title: String, by: String, snapshots: List[StorySnapshot]) {}
  case class StorySnapshot(ts: LocalDateTime, score: Int)                                                       {}

  case class RawStory(id: Long, created: Long, title: String, by: String, snap: Long, score: Int)

  def loadTop(file: String): List[Top] = {
    val reader = new FileReader(file)
    val csv    = new CSVReaderBuilder(reader).build()
    val records = csv.iterator().asScala.map(_.toList).collect { case head :: tail =>
      Top(time(head.toLong), tail.map(_.toLong).toArray)
    }
    val result = records.toList
    println(s"loaded $file")
    result
  }

  val start = LocalDateTime.of(2022, 9, 1, 0, 0)
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
    raw
      .groupMapReduce[Long, Story](_.id)(r =>
        Story(r.id, time(r.created), r.title, r.by, List(StorySnapshot(time(r.snap), r.score)))
      )((a, b) => a.copy(snapshots = a.snapshots ++ b.snapshots))
      .filter(_._2.created.isAfter(start))
      .map(kv => kv._1 -> kv._2.copy(snapshots = kv._2.snapshots.sortBy(_.ts)))
      .filter(_._1 != 32768834)
  }

  def load(dir: String) = Dataset(
    news = loadTop(s"$dir/newstories.csv"),
    best = loadTop(s"$dir/beststories.csv"),
    show = loadTop(s"$dir/showstories.csv"),
    top = loadTop(s"$dir/topstories.csv"),
    stories = loadStories(s"$dir/items.csv")
  )

  def time(millis: Long): LocalDateTime = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.from(ZoneOffset.UTC))
  }

}
