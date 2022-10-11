package ai.metarank.hnscrape.report.impl

import ai.metarank.hnscrape.report.Dataset

trait Report {
  def generate(ds: Dataset, path: String): Unit
}
