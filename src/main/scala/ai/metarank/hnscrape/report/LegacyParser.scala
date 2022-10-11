package ai.metarank.hnscrape.report

import ai.metarank.hnscrape.HNAPI.ItemTimestamp
import ai.metarank.hnscrape.Item
import better.files.File
import com.opencsv.CSVWriterBuilder
import io.circe.Codec
import io.circe.generic.semiauto._
import io.circe.parser._
import io.circe.syntax._
import scala.jdk.CollectionConverters._
import java.io.FileWriter
import scala.collection.parallel.CollectionConverters._

object LegacyParser {
  case class StoryHistory(by: String, id: Long, time: Long, tpe: String, title: String, hist: List[History])
  case class History(ts: Long, score: Int, comments: Int)
  implicit val histCodec: Codec[History]       = deriveCodec[History]
  implicit val storyCodec: Codec[StoryHistory] = deriveCodec[StoryHistory]

  object StoryHistory {
    def apply(item: Item, ts: Long): Option[StoryHistory] = for {
      title    <- item.title
      score    <- item.score
      comments <- item.descendants
    } yield {
      StoryHistory(
        by = item.by,
        id = item.id,
        time = item.time,
        title = title,
        tpe = item.`type`,
        hist = List(History(ts = ts, score = score, comments = comments))
      )
    }
  }

  object History {
    def apply(item: Item, ts: Long): Option[History] = for {
      score    <- item.score
      comments <- item.descendants
    } yield {
      History(ts, score, comments)
    }
  }

  case class BestEntry(ts: Long, stories: List[Long]) {
    def asLine: Array[String] = (ts.toString +: stories.map(_.toString)).toArray
  }

  def main(args: Array[String]): Unit = {
    args.toList match {
      case path :: out :: Nil =>
        parseItems(path, "items", out)
//        parseBest(path, "topstories", out)
//        parseBest(path, "beststories", out)
//        parseBest(path, "showstories", out)
//        parseBest(path, "newstories", out)
    }
  }

  def parseBest(dir: String, subdir: String, out: String) = {
    val writer = new CSVWriterBuilder(new FileWriter(s"$out/$subdir.csv")).withSeparator(',').build()
    for {
      file <- (File(dir) / subdir)
        .list(_.extension(includeDot = false).contains("json"))
        .toList
        .sortBy(_.nameWithoutExtension.toLong)
      content <- decode[List[Long]](file.contentAsString).toOption
    } {
      val entry = BestEntry(file.nameWithoutExtension.toLong, content)
      writer.writeNext(entry.asLine)
    }
    writer.close()
  }

  def parseItems(dir: String, subdir: String, out: String) = {
    val target = File(dir) / subdir
    val items  = (File(out) / "items")
    items.createDirectoryIfNotExists()
    val targets = target.list.toList
    val cnt     = targets.size
    val writer  = new FileWriter(s"$out/items.csv", true)
    val csv     = new CSVWriterBuilder(writer).withSeparator(',').build()

    var xcnt = 0
    for {
      item <- targets
    } {
      val snapshots = item
        .list(_.extension(includeDot = false).contains("json"))
        .toList
        .flatMap(f =>
          decode[Item](f.contentAsString).toOption.map(i => ItemTimestamp(i, f.nameWithoutExtension.toLong).asCSVLine)
        )
        .asJava
      csv.writeAll(snapshots)
      xcnt += 1
      if (xcnt % 123 == 0) println(s"done $xcnt/$cnt items")
    }
    csv.close()
    writer.close()
  }
}
