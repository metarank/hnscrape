package ai.metarank.hnscrape

import io.circe.Codec
import io.circe.generic.semiauto._

case class Item(
    by: String,
    descendants: Option[Int],
    id: Int,
    kids: Option[List[Int]],
    parts: Option[List[Int]],
    parent: Option[Int],
    score: Option[Int],
    time: Long,
    title: Option[String],
    url: Option[String],
    text: Option[String],
    `type`: String
)

object Item {

  implicit val itemCodec: Codec[Item] = deriveCodec[Item]

}
