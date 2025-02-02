package backend

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object MainApp {
  implicit val system: ActorSystem = ActorSystem("SparkAkkaHttpServer")
  implicit val materializer: Materializer = Materializer(system)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkAkkaHttpBackend")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val sparkService = new SparkService(spark)

    val apiRoutes = new ApiRoutes(sparkService)

    val bindingFuture = Http().newServerAt("0.0.0.0", 8080).bind(apiRoutes.routes)

    println("Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine()
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => system.terminate())
  }
}
