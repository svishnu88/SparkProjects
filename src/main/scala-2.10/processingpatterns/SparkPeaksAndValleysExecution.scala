package processingpatterns


import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.{Partitioner, SparkContext, SparkConf}

import scala.collection.mutable

/**
  * Created by vishnu on 06/04/16.
  */
object SparkPeaksAndValleysExecution {

  def main(args: Array[String]) {

    val localInputPath = "/Users/vishnu/Documents/IdeaProjects/SparkProjects/src/main/resources/peaksandvalley/peaksandvalley.txt"
    val localOutputPath = "/Users/vishnu/Documents/IdeaProjects/SparkProjects/src/main/resources/peaksandvalley/peaksandvalleyoutput"


    val Array(inputPath: String, outputPath: String, numberOfPartitions: Int) = if (args.length == 0) {
      Array(localInputPath, localOutputPath, 10)
    } else {
      Array(args(0), args(1), args(2).toInt)
    }

    val sparkConf = new SparkConf()
    sparkConf.setAppName("SparkTimeSeriesExecution")
    sparkConf.setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    //Reading Data

    val originalDataRDD = sc.hadoopFile(inputPath,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      1).map(r => {
      val splits = r._2.toString.split(",")
      (new DataKey(splits(0), splits(1).toLong), splits(2).toInt)
    })

    //Partitioner to partition by primary key

    val partitioner = new Partitioner {

      override def numPartitions: Int = numberOfPartitions

      override def getPartition(key: Any): Int = {
        Math.abs(key.asInstanceOf[DataKey].uniqueId.hashCode % numPartitions)
      }
    }

    // Partition and Sort

    val partedSortedRDD = new ShuffledRDD[DataKey, Int, Int](
      originalDataRDD,
      partitioner).setKeyOrdering(implicitly[Ordering[DataKey]])

    //MapPartiton to do windowing

    val pivotPointRDD = partedSortedRDD.mapPartitions(it => {
      val results = new mutable.MutableList[PivotPoint]

      //keeping context

      var lastUniqueId = "foobar"
      var lastrecord: (DataKey, Int) = null
      var lastLastRecord: (DataKey, Int) = null

      var position = 0

      it.foreach(r => {
        position = position + 1

        if (!lastUniqueId.equals(r._1.uniqueId)) {

          lastrecord == null
          lastLastRecord == null
        }

        // Finding Peaks and Values

        if (lastrecord != null && lastLastRecord != null) {
          if (lastrecord._2 < r._2 && lastrecord._2 < lastLastRecord._2) {
            results.+=(new PivotPoint(r._1.uniqueId,
              position,
              lastrecord._1.eventTime,
              lastrecord._2,
              false
            ))
          } else if (lastrecord._2 > r._2 && lastLastRecord._2 > r._2) {
            results.+=(new PivotPoint(r._1.uniqueId,
              position,
              lastrecord._1.eventTime,
              lastrecord._2,
              true
            ))
          }
        }

        lastUniqueId = r._1.uniqueId
        lastLastRecord = lastrecord
        lastrecord = r

      })

      results.iterator

    })

    //pretty things up

    pivotPointRDD.map(r => {
      val pivotType = if (r.isPeak) "peak" else "valley"
      r.uniqueId + "," +
        r.position + "," +
        r.eventTime + "," +
        r.eventValue + "," +
        pivotType
    }).saveAsTextFile(outputPath)

  }

}

class DataKey(val uniqueId: String, val eventTime: Long) extends Serializable with Comparable[DataKey] {
  override def compareTo(other: DataKey): Int = {
    val compare1 = uniqueId.compareTo(other.uniqueId)
    if (compare1 == 0) {
      eventTime.compareTo(other.eventTime)
    }
    else {
      compare1
    }
  }
}

class PivotPoint(val uniqueId: String,
                 val position: Int,
                 val eventTime: Long,
                 val eventValue: Int,
                 val isPeak: Boolean
                ) extends Serializable {}


