package ai.metarank.hnscrape

import ai.metarank.hnscrape.HNAPI.{ItemTimestamp, ListingTimestamp}
import cats.effect.{ExitCode, IO, IOApp}
import com.opencsv.{CSVReaderBuilder, CSVWriterBuilder}
import io.circe.syntax._

import scala.concurrent.duration._
import fs2.Stream
import fs2.io.file.{Files, Flags, Path}
import io.circe.{Json, Printer}

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Date
import scala.util.Random

object Main extends IOApp with Logging {

  val jsonFormat = Printer.spaces2.copy(dropNullValues = true)
  override def run(args: List[String]): IO[ExitCode] = {
    HNAPI
      .create()
      .use(api =>
        for {
          path <- IO.fromOption(args.headOption)(new Exception("path missing"))
          _    <- info("Started the main loop")
          _    <- stream(api, path).compile.drain
        } yield {
          ExitCode.Success
        }
      )
  }

  def stream(client: HNAPI, path: String) =
    Stream
      .repeatEval(targets(client, path).handleErrorWith(ex => error("cannot poll targets", ex) *> IO.pure(Nil)))
      .evalTap(ids => info(s"${ids.size} targets to scrape"))
      .flatMap(Stream.emits)
      .metered(500.millis)
      .evalMap(id =>
        client.item(id).flatMap(saveItem(_, path)).handleErrorWith(ex => error("cannot get item", ex) *> IO.unit)
      )

  def targets(client: HNAPI, path: String): IO[List[Int]] = for {
    news <- client.newstories().flatTap(saveListing(_, path))
    top  <- client.topstories().flatTap(saveListing(_, path))
    show <- client.showtories().flatTap(saveListing(_, path))
    best <- client.beststories().flatTap(saveListing(_, path))
  } yield {
    Random.shuffle(List.concat(news.items, top.items, show.items, best.items).distinct)
  }

  val format = DateTimeFormatter.ofPattern("yyyy_MM_dd").withZone(ZoneId.from(ZoneOffset.UTC))

  def saveListing(list: ListingTimestamp, path: String): IO[Unit] = IO {
    val ts     = Instant.ofEpochMilli(list.ts)
    val writer = new FileWriter(s"$path/${format.format(ts)}_${list.name}.csv", true)
    val csv    = new CSVWriterBuilder(writer).withSeparator(',').build()
    csv.writeNext(list.asCSVLine)
    csv.close()
    writer.close()
  }

  def saveItem(item: ItemTimestamp, path: String): IO[Unit] = IO {
    val ts     = Instant.ofEpochMilli(item.ts)
    val writer = new FileWriter(s"$path/${format.format(ts)}_items.csv", true)
    val csv    = new CSVWriterBuilder(writer).withSeparator(',').build()
    csv.writeNext(item.asCSVLine)
    csv.close()
    writer.close()
  }
}
