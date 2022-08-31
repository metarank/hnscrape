package ai.metarank.hnscrape

import ai.metarank.hnscrape.HNAPI.{ItemTimestamp, ListingTimestamp}
import cats.effect.{IO, Resource}
import org.http4s.Uri
import org.http4s.blaze.client._
import org.http4s.circe._
import org.http4s.client._
import scala.concurrent.duration._

case class HNAPI(client: Client[IO], endpoint: Uri) extends Logging {
  implicit val itemDecoder = jsonOf[IO, Item]
  implicit val listDecoder = jsonOf[IO, List[Int]]

  def item(id: Int): IO[ItemTimestamp] = for {
    start  <- IO(System.currentTimeMillis())
    result <- client.get(endpoint / "item" / s"$id.json")(_.as[Item])
    _      <- info(s"get $id (${System.currentTimeMillis() - start}ms, title: ${result.title.getOrElse("")})")
  } yield {
    ItemTimestamp(result, start)
  }

  def topstories(): IO[ListingTimestamp] = list("topstories")

  def newstories(): IO[ListingTimestamp] = list("newstories")

  def showtories(): IO[ListingTimestamp] = list("showstories")

  def beststories(): IO[ListingTimestamp] = list("beststories")

  def list(part: String): IO[ListingTimestamp] = for {
    start  <- IO(System.currentTimeMillis())
    result <- client.get(endpoint / s"$part.json")(_.as[List[Int]])
    _      <- info(s"poll $part (${System.currentTimeMillis() - start}ms, ${result.size} items)")
  } yield {
    ListingTimestamp(result, start, part)
  }
}

object HNAPI extends Logging {
  case class ItemTimestamp(item: Item, ts: Long)
  case class ListingTimestamp(items: List[Int], ts: Long, name: String)

  def create() = for {
    client   <- BlazeClientBuilder[IO].withConnectTimeout(1.second).withRequestTimeout(1.second).resource
    endpoint <- Resource.liftK(IO.fromEither(Uri.fromString("https://hacker-news.firebaseio.com/v0/")))
    _        <- Resource.liftK(info("HTTP Client created"))
  } yield {
    HNAPI(client, endpoint)
  }
}
