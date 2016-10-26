package spark.example

import com.google.gson.Gson
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import redis.clients.jedis.{JedisPool, Jedis}

/**
 * Created by brobrien on 10/26/16.
 */
object MyStreamProcessor {

  val gson = new Gson()

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

  def processStream(stream: ReceiverInputDStream[(String, String)]): Unit = {

    //DStream is just a sequence of RDD's
    stream.foreachRDD(rdd => {
      //This function executed on the driver

      rdd.foreachPartition(partition => {
        //This function executed on the executor(s)

        //RDD partitions are processed in parallel, but elements in a single partition are processed serially
        partition.foreach(message => {
          println("Received message: " + message._2)
          JedisProvider.exec(jedis => {
            jedis.publish("raw-messages", message._2)
          })
        })

      })

    })
  }
}

case class StreamTransition(from: String, to: String, count: Int)