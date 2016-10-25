
package spark.example

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * Example program to consume and process streaming data with Spark Streaming
 *
 * See http://spark.apache.org/docs/latest/streaming-programming-guide.html for more info
 *
 * Use "nc -lk 1337" to create local TCP server
 */

object SparkRunner {

  val tcpPort = 1337
  val batchDurationMilliseconds = new Duration(3 * 1000)

  def main(args: Array[String]) = {
    println("Setting up the Spark app")

    TCPDataSource.initDataStream(tcpPort)

    val ssc: StreamingContext = createStreamingContext()
    val stream: ReceiverInputDStream[String] = createStream(ssc)

    processStream(stream)
    //processStreamCountWordLength(stream)

    println("Transformation sequence created")
    ssc.start()
    ssc.awaitTermination()
  }

  def processStreamCountWordLength(stream: ReceiverInputDStream[String]): Unit = {
    val wordLengths: DStream[(Int, Int)] = stream.map(word => (word.length, 1))

    val countsByLength: DStream[(Int, Int)] = wordLengths.reduceByKey((w1, w2) => w1 + w2)

    //dstream.foreachRDD triggers actual execution
    countsByLength.foreachRDD(rdd => {
      println("new RDD")
      rdd.collect().foreach(wordLengthCount => {
        val length = wordLengthCount._1
        val count = wordLengthCount._2
        println(s"$count words with length $length observed")
      })
    })
  }

  def processStream(stream: ReceiverInputDStream[String]): Unit = {

    //DStream is just a sequence of RDD's
    stream.foreachRDD(rdd => {
      //This function executed on the driver

      rdd.foreachPartition(partition => {
        //This function executed on the executor(s)

        //RDD partitions are processed in parallel, but elements in a single partition are processed serially
        partition.foreach(message => {
          println("Received message: " + message)
        })

      })

    })
  }

  def createStream(ssc: StreamingContext): ReceiverInputDStream[String] = {
    ssc.socketTextStream("localhost", tcpPort)
  }

  def createStreamingContext(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("streaming-example")
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.default.parallelism", Runtime.getRuntime.availableProcessors.toString())
    new StreamingContext(sparkConf, batchDurationMilliseconds)
  }
}