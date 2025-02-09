package backend

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._

class ApiRoutes(sparkService: SparkService) extends JsonFormats {

  val routes = cors() {
    pathPrefix("api") {
      concat(
        path("stations") {
          get {
            complete(sparkService.getAllStations.toJson)
          }
        },
        path("zones") {
          get {
            complete(sparkService.getAllZones.toJson)
          }
        },
        path("zonets") {
          get {
            parameter("time") { timeParam =>
              complete(sparkService.getZoneTSByTime(timeParam).toJson)
            }
          }
        }
      )
    }
  }
}
