package ai.metarank.hnscrape

import ai.metarank.hnscrape.HNAPI.{ItemTimestamp, ListingTimestamp}
import cats.effect.{ExitCode, IO, IOApp}
import io.circe.syntax._

import scala.concurrent.duration._
import fs2.Stream
import fs2.io.file.{Files, Flags, Path}
import io.circe.{Json, Printer}

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

  def saveListing(list: ListingTimestamp, path: String): IO[Unit] = {
    save(
      Json.fromValues(list.items.map(i => Json.fromInt(i))),
      dir = s"$path/${list.name}",
      name = s"${list.ts}.json"
    )
  }

  def saveItem(item: ItemTimestamp, path: String): IO[Unit] = {
    save(item.item.asJson, s"$path/items/${item.item.id}", s"${item.ts}.json")
  }

  def save(json: Json, dir: String, name: String): IO[Unit] = {
    for {
      exists <- Files[IO].exists(Path(dir))
      _      <- IO.whenA(!exists)(Files[IO].createDirectories(Path(dir)))
      _ <- Stream
        .emits(jsonFormat.print(json).getBytes())
        .through(Files[IO].writeAll(Path(s"$dir/$name"), Flags.Write))
        .compile
        .drain
    } yield {}
  }
}
