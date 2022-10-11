package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder
import org.apache.commons.math3.stat.descriptive.rank.Percentile

import java.io.FileWriter
import java.time.ZoneOffset

object UpvotesDay extends Report {
  override def generate(ds: Dataset, path: String): Unit = {
    val csv = new CSVWriterBuilder(new FileWriter(s"$path/upvotes_day.csv")).withSeparator(',').build()

    val upvotes = ds.stories.values
      .filter(_.snapshots.size > 1)
      .flatMap(s =>
        s.snapshots.sortBy(_.ts.toInstant(ZoneOffset.UTC).toEpochMilli).sliding(2).map { case a :: b :: Nil =>
          a.ts -> (b.score - a.score)
        }
      )
      .groupBy(x => startOfHour(x._1))
      .map { case (hour, incs) =>
        hour -> incs.map(_._2).sum
      }
      .toList
      .groupBy(x => x._1.getHour)
      .map(kv => kv._1 -> kv._2.map(_._2))
      .toList
      .sortBy(_._1)
    upvotes.foreach { case (hour, cnt) =>
      val avg = cnt.sum.toDouble / cnt.size
      val p   = new Percentile()
      p.setData(cnt.map(_.toDouble).toArray)
      val median = p.evaluate(50)
      val p90    = p.evaluate(90)
      val p10    = p.evaluate(10)
      csv.writeNext(Array(hour.toString, avg.toString, median.toString, p90.toString, p10.toString))
    }
    csv.close()
  }
}
