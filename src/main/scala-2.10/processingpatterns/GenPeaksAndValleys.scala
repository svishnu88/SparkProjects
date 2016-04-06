package processingpatterns


import java.io.{OutputStreamWriter, BufferedWriter}
import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by vishnu on 06/04/16.
  */
object GenPeaksAndValleys {

  def main(args: Array[String]) {


    if (args.length == 0) {
      println("{outputPath} {numberOfRecords} {numberOfUniqueIds}")
      println("Default values or being taken")
    }
    val localOutPutPath = "/Users/vishnu/Documents/IdeaProjects/SparkProjects/src/main/resources/peaksandvalley/peaksandvalley.txt"



    val Array(outputPath: Path, numberOfRecords: Int, numberOfUniqueIds: Int) = if (args.length == 0) {
      Array(new Path(localOutPutPath), 1000, 10)
    } else {
      Array(new Path(args(0)), args(1).toInt, args(2).toInt)
    }



    val fileSystem = FileSystem.get(new Configuration())

    val writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(outputPath)))

    val r = new Random()

    var direction = 1
    var directionCounter = r.nextInt(numberOfUniqueIds * 10)
    var currentPointer = 0

    for (i <- 0 until numberOfRecords) {
      val uniqueId = r.nextInt(numberOfUniqueIds)

      currentPointer = currentPointer + direction
      directionCounter = directionCounter - 1
      if (directionCounter == 0) {
        directionCounter = r.nextInt(numberOfUniqueIds * 10)
        direction = -1
      }

      writer.write(uniqueId + "," + i + "," + currentPointer)
      writer.newLine()
    }
    writer.close()
  }

}
