package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import cats.data.NonEmptyList
import com.opencsv.CSVWriterBuilder
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

import java.io.FileWriter
import scala.collection.mutable.ArrayBuffer

object Keywords extends Report {
  case class StoryTitle(id: Long, tokens: Set[String])
  case class TermProb(term: String, main: Int, miss: Int, prob: Double, delta: Double)
  object TextAnalyzer {
    lazy val analyzer = new EnglishAnalyzer()

    def split(line: String): Array[String] = {
      val stream = analyzer.tokenStream("ye", line)
      stream.reset()
      val term   = stream.addAttribute(classOf[CharTermAttribute])
      val buffer = ArrayBuffer[String]()
      while (stream.incrementToken()) {
        val next = term.toString
        buffer.append(next)
      }
      stream.close()
      buffer.toArray
    }
  }

  override def generate(ds: Dataset, path: String): Unit = {
    val stories = ds.stories.values.toList.map(s => StoryTitle(s.id, TextAnalyzer.split(s.title).toSet))

    val terms = stories
      .flatMap(_.tokens)
      .groupMapReduce(identity)(_ => 1)(_ + _)
      .toList
      .sortBy(_._2)
      .reverse
      .take(100)
      .map(_._1)

    val topStories = ds.top.flatMap(_.stories.take(30)).toSet

    val avgProb = topStories.size.toDouble / stories.size

    val termProb = terms
      .map(term => {
        val total   = stories.filter(_.tokens.contains(term))
        val hitMain = total.count(s => topStories.contains(s.id))
        val nonMain = total.count(s => !topStories.contains(s.id))
        val prob    = hitMain / total.size.toDouble
        TermProb(term, hitMain, nonMain, prob, prob - avgProb)
      })
      .sortBy(_.delta)
    val csv = new CSVWriterBuilder(new FileWriter(s"$path/keywords.csv")).withSeparator(',').build()
    termProb.foreach(tp =>
      csv.writeNext(Array(tp.term, tp.main.toString, tp.miss.toString, tp.prob.toString, tp.delta.toString))
    )
    csv.close()
  }
}
