package spark.example

import java.util.UUID

/**
 * Created by brobrien on 10/25/16.
 */
object TCPDataSource {

  import java.io._
  import java.net._

  def initDataStream(port: Int) = {
    new Thread(new Runnable {
      def run(): Unit = {
        val server = new ServerSocket(port)
        while (true) {
          println("Server waiting")
          val s: Socket = server.accept()
          println("Client connected")
          var i = 0
          val out = new PrintStream(s.getOutputStream())
          while (i < 50) {
            out.println(UUID.randomUUID().toString.substring(0, 6))
            out.flush()
            i += 1
            try {
              Thread.sleep(300)
            } catch {
              case e: InterruptedException => {}
            }
          }
          s.close()
        }
      }
    }).start
    println("TCP data stream initialized")
  }
}
