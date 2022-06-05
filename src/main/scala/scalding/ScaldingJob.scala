package org.jeremy.spark
package scalding

import com.twitter.scalding.{Args, Csv, Job, TextLine, TypedPipe}
import com.twitter.scalding._
import com.twitter.algebird.Aggregator.size


class ScaldingJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedCsv[(Long, String)](args("inputPath")))
    .groupBy(x => x).aggregate(size)
    .filter(row => row._1._1 > 1L)
    .map(row => (row._1._1, row._1._2, row._2))
    .write(TypedCsv[(Long, String, Long)](args("outputPath")))
}
