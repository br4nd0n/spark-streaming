package spark.example

import com.google.gson.Gson
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by brobrien on 10/26/16.
 */
object MyStreamProcessor {

  val gson = new Gson()

  def processStreamCountWordLength(stream: DStream[String]): Unit = {

    stream.foreachRDD(rdd => {

      val batchOfWords = rdd.collect() //normally you wouldn't do this in a production scenario
      val totalWords = batchOfWords.length
      val totalCharacters = batchOfWords.foldLeft(0) { (sum, word) => sum + word.length }
      val avgCharsPerWord = if (totalWords == 0) 0 else (totalCharacters.toDouble / totalWords)

      val summaryMetrics = SummaryMetrics(totalWords, totalCharacters, avgCharsPerWord)

      val jsonSummaryData = gson.toJson(summaryMetrics)
      println(jsonSummaryData)
      JedisProvider.exec(_.publish("summary-metrics", jsonSummaryData))

      JedisProvider.exec(jedis => {
        batchOfWords.foreach(word => {
          jedis.publish("raw-messages", word)
        })
      })
    })

    val wordLengths: DStream[(Int, Int)] = stream.map(word => (word.length, 1))
    val countsByLength: DStream[(Int, Int)] = wordLengths.reduceByKey((w1, w2) => w1 + w2)

    //dstream.foreachRDD triggers actual execution
    countsByLength.foreachRDD(rdd => {
      println("new RDD")

      val countsCollection = for (wordLengthCount <- rdd.collect()) yield {
        val length = wordLengthCount._1
        val count = wordLengthCount._2
        println(s"$count words with length $length observed")
        DetailCount(length, count)
      }
      val jsonDetailData = gson.toJson(countsCollection)
      JedisProvider.exec(_.publish("detail-metrics", jsonDetailData))
    })
  }

  def processStream(stream: DStream[String]): Unit = {
    //DStream is just a sequence of RDD's
    stream.foreachRDD(rdd => {
      //This function executed on the driver

      rdd.foreachPartition(partition => {
        //This function executed on the executor(s)

        //RDD partitions are processed in parallel, but elements in a single partition are processed serially
        partition.foreach(message => {
          println("Received message: " + message)
          JedisProvider.exec(jedis => {
            jedis.publish("raw-messages", message)
          })
        })
      })
    })
  }
}

case class SummaryMetrics(totalWords: Int, totalCharacters: Int, avgCharsPerWord: Double)

case class DetailCount(wordLength: Int, count: Int)

case class StreamTransition(from: String, to: String, count: Int)