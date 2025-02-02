package backend

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._

class ApiRoutes(sparkService: SparkService) extends JsonFormats {
  val routes = pathPrefix("api") {
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
      }
    )
  }
}
