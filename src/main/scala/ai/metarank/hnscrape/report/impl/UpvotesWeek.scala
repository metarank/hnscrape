package ai.metarank.hnscrape.report.impl
import ai.metarank.hnscrape.report.Dataset
import com.opencsv.CSVWriterBuilder

import java.io.FileWriter
import java.time.ZoneOffset

object UpvotesWeek extends Report {
  override def generate(ds: Dataset, path: String): Unit = {
    val csv = new CSVWriterBuilder(new FileWriter(s"$path/upvotes_seasonal.csv")).withSeparator(',').build()
    val top = ds.stories.values.toList.sortBy(_.snapshots.map(_.score).max).reverse
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
      .sortBy(_._1)
    upvotes.foreach { case (hour, cnt) =>
      csv.writeNext(Array(format.format(hour), cnt.toString))
    }
    csv.close()
  }
}
