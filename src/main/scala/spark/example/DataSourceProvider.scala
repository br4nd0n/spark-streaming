package spark.example

/**
 * Created by brobrien on 10/25/16.
 */
object DataSourceProvider {

  import java.io._
  import java.net._

  //Read the file into memory and convert into an array of words, with punctuation removed and all-lowercase
  val stream = getClass().getResourceAsStream("/hitchhikers.txt")
  val corpus = scala.io.Source.fromInputStream(stream).mkString
  val corpusNoPunctuation = corpus.replaceAll("[^A-Za-z0-9\\s]", "")
  val words = corpusNoPunctuation.split("\\s+").map(_.toLowerCase)

  def main(args: Array[String]): Unit = {
    initRedisDataStream("words")
  }

  def initRedisDataStream(messageSet: String) = {
    println("Initializing Redis Data Stream Producer")

    new Thread(new Runnable {
      def run(): Unit = {
        var i = 0
        while (i < words.length) {
          val word = words(i)
          JedisProvider.exec(jedis => {
            jedis.sadd(messageSet, word)
          })
          i += 1
          try {
            Thread.sleep(300)
          } catch {
            case e: InterruptedException => {}
          }
        }
      }
    }).start
  }

  def initTCPDataStream(port: Int) = {
    new Thread(new Runnable {
      def run(): Unit = {
        println(s"Creating server on port $port")
        val server = new ServerSocket(port)

        while (true) {
          println(s"Server waiting. Bound? ${server.isBound} ${server.getLocalPort}")
          val s: Socket = server.accept()

          println(s.getRemoteSocketAddress)
          println("Client connected")
          handleClient(s)
        }
      }
    }).start
    println("TCP data stream initialized")
  }

  def handleClient(s: Socket): Unit = {
    new Thread(new Runnable {
      def run(): Unit = {
        var i = 0
        val out = new PrintStream(s.getOutputStream())
        while (i < words.length && s.isConnected) {
          val word = words(i)
          //println(s"Sending $word")
          out.println(word) //for random text: UUID.randomUUID().toString.substring(0, 6)
          out.flush()
          i += 1
          try {
            Thread.sleep(300)
          } catch {
            case e: InterruptedException => {}
          }
        }
        //s.close()
      }
    }).start
  }

}
