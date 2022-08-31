package ai.metarank.hnscrape

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.parser._

class StoryJsonTest extends AnyFlatSpec with Matchers {
  it should "parse story from docs" in {
    val json =
      """{
        |  "by" : "dhouston",
        |  "descendants" : 71,
        |  "id" : 8863,
        |  "kids" : [ 8952, 9224, 8917, 8884, 8887, 8943, 8869, 8958, 9005, 9671, 8940, 9067, 8908, 9055, 8865, 8881, 8872, 8873, 8955, 10403, 8903, 8928, 9125, 8998, 8901, 8902, 8907, 8894, 8878, 8870, 8980, 8934, 8876 ],
        |  "score" : 111,
        |  "time" : 1175714200,
        |  "title" : "My YC app: Dropbox - Throw away your USB drive",
        |  "type" : "story",
        |  "url" : "http://www.getdropbox.com/u/2/screencast.html"
        |}""".stripMargin
    val result = decode[Item](json)
    result shouldBe a[Right[_, Item]]
  }

  it should "parse ask" in {
    val json =
      """{
        |  "by" : "tel",
        |  "descendants" : 16,
        |  "id" : 121003,
        |  "kids" : [ 121016, 121109, 121168 ],
        |  "score" : 25,
        |  "text" : "<i>or</i> HN: the Next Iteration<p>I get the impression that with Arc being released a lot of people who never had time for HN before are suddenly dropping in more often. (PG: what are the numbers on this? I'm envisioning a spike.)<p>Not to say that isn't great, but I'm wary of Diggification. Between links comparing programming to sex and a flurry of gratuitous, ostentatious  adjectives in the headlines it's a bit concerning.<p>80% of the stuff that makes the front page is still pretty awesome, but what's in place to keep the signal/noise ratio high? Does the HN model still work as the community scales? What's in store for (++ HN)?",
        |  "time" : 1203647620,
        |  "title" : "Ask HN: The Arc Effect",
        |  "type" : "story"
        |}""".stripMargin
    val result = decode[Item](json)
    result shouldBe a[Right[_, Item]]
  }

  it should "parse no comments story" in {
    val json = """{
                 |  "by" : "mupuff1234",
                 |  "descendants" : 0,
                 |  "id" : 32660913,
                 |  "score" : 1,
                 |  "time" : 1661946211,
                 |  "title" : "Ask HN: Best MOOCs You've Taken?",
                 |  "type" : "story"
                 |}""".stripMargin
    val result = decode[Item](json)
    result shouldBe a[Right[_, Item]]
  }
}
