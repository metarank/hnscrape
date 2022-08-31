package ai.metarank.hnscrape

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.parser._

class CommentJsonTest extends AnyFlatSpec with Matchers {
  it should "parse comment from doc" in {
    val json =
      """{
        |  "by" : "norvig",
        |  "id" : 2921983,
        |  "kids" : [ 2922097, 2922429, 2924562, 2922709, 2922573, 2922140, 2922141 ],
        |  "parent" : 2921506,
        |  "text" : "Aw shucks, guys ... you make me blush with your compliments.<p>Tell you what, Ill make a deal: I'll keep writing if you keep reading. K?",
        |  "time" : 1314211127,
        |  "type" : "comment"
        |}""".stripMargin
    val result = decode[Item](json)
    result shouldBe a[Right[_, _]]
  }
}
