import jdk.nashorn.internal.runtime.JSONFunctions

import scala.util.parsing.json._


case class HttpLog(agent: String,
                   auth: String,
                   ident: String,
                   verb: String,
                   message: String,
                   timestamp : String,
                   response : Int,
                   bytes : Int,
                   clientip : String,
                   httpversion : String,
                   request : String,
                   referrer : String,
                   host : String
                  )

case class HdfsSourceValue(path: String, dataSource: String, dataset: String)

object HttpLogSchema extends App {
  import com.sksamuel.avro4s.AvroSchema
  val schema = AvroSchema[HdfsSourceValue]

  println(JSONFunctions.quote(schema.toString))
  println(schema)
}