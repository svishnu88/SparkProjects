import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by vishnu on 04/04/16.
  */
object WordCount {

  // For checking if my spark env is working on Idea.

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val path = "/Users/vishnu/Documents/IdeaProjects/SparkProjects/src/main/resources/WCSample"
    val rdd = sc.textFile(path)
    val wc = rdd.flatMap(_.split(" ")).map(x => (x,1)).reduceByKey(_+_)
    println(wc.first())

  }

}
