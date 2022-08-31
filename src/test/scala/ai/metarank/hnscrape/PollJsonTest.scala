package ai.metarank.hnscrape

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.parser._

class PollJsonTest extends AnyFlatSpec with Matchers {
  it should "decode poll from doc" in {
    val json =
      """{
        |  "by" : "pg",
        |  "descendants" : 54,
        |  "id" : 126809,
        |  "kids" : [ 126822, 126823, 126993, 126824, 126934, 127411, 126888, 127681, 126818, 126816, 126854, 127095, 126861, 127313, 127299, 126859, 126852, 126882, 126832, 127072, 127217, 126889, 127535, 126917, 126875 ],
        |  "parts" : [ 126810, 126811, 126812 ],
        |  "score" : 46,
        |  "text" : "",
        |  "time" : 1204403652,
        |  "title" : "Poll: What would happen if News.YC had explicit support for polls?",
        |  "type" : "poll"
        |}""".stripMargin
    val result = decode[Item](json)
    result shouldBe a[Right[_, Item]]

  }
}
