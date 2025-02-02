package backend

import spray.json._

trait JsonFormats extends DefaultJsonProtocol {
  implicit val mapFormat: JsonFormat[Map[String, String]] = new JsonFormat[Map[String, String]] {
    def write(m: Map[String, String]): JsValue = JsObject(m.mapValues(JsString(_)))

    def read(value: JsValue): Map[String, String] = value match {
      case JsObject(fields) => fields.map { case (k, v) => k -> v.convertTo[String] }
      case _ => throw DeserializationException("Expected a JSON object")
    }
  }
}
