package ai.metarank.hnscrape

import better.files.File
import com.opencsv.{CSVWriter, CSVWriterBuilder}
import io.circe.Codec
import io.circe.parser._
import io.circe.generic.semiauto._
import io.circe.syntax._
import java.io.FileWriter

object Parse {
  case class StoryHistory(by: String, id: Long, time: Long, title: String, hist: List[History])
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
    for {
      (item, index) <- targets.zipWithIndex
    } yield {
      val snapshots = item
        .list(_.extension(includeDot = false).contains("json"))
        .toList
        .flatMap(f => decode[Item](f.contentAsString).toOption.map(i => i -> f.nameWithoutExtension.toLong))
      snapshots match {
        case head :: tail =>
          StoryHistory(head._1, head._2) match {
            case None =>
            case Some(story) =>
              val result = tail
                .flatMap(x => History(x._1, x._2))
                .foldLeft(story)((acc, hist) => acc.copy(hist = hist +: acc.hist))
              val f        = result.copy(hist = result.hist.sortBy(_.ts))
              val itemFile = File(items / item.nameWithoutExtension + ".json")
              itemFile.writeText(f.asJson.spaces2)
          }
        case _ => //
      }
      if (index % 123 == 0) println(s"done $index/$cnt items")
    }
  }
}
