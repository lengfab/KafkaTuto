package app.restService


import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.HostInfo
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import app.{DataCpt, DataCptForDate}
import app.utils.Utils
import org.apache.kafka.common.serialization.Serdes
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import org.apache.kafka.streams.state.QueryableStoreTypes

object RestService {
  val DEFAULT_REST_ENDPOINT_HOSTNAME  = "localhost"
}


class TopRestService(val streams: KafkaStreams, val hostInfo: HostInfo) {

  val metadataService = new MetadataService(streams)
  var bindingFuture: Future[Http.ServerBinding] = null

  implicit val system = ActorSystem("rating-system")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  val format = new SimpleDateFormat("dd-MM-yy", Locale.ENGLISH)

  // formats for unmarshalling and marshalling
  implicit val dataCptFormat = jsonFormat2(DataCpt)
  implicit val dataCptForDateFormat = jsonFormat2(DataCptForDate)

  val NB_URL_RETURNED : Int = 10
  val NB_AGENT_RETURNED : Int = 3

  def start() : Unit = {
    val route =
      path("streaming-kafka") {
        get {
          parameters('store.as[String], 'date.as[String]) { (store, date) =>

            val key = Utils.getKey(format.parse(date)).toString

            val host = metadataService.streamsMetadataForStoreAndKey[String](
                store,
                key,
                Serdes.String().serializer()
              )

              var future:Future[DataCptForDate] = null

              //store is hosted on another process, REST Call
//              if(!thisHost(host))
//                future = fetchRemoteRatingByEmail(host, date)
//              else
                future = fetchLocalTopUrl(store, key)

//              val ratings = Await.result(future, 20 seconds)
//              complete(ratings)
              onComplete(future) {
                case Success(dataCptForDate) => {
                  val nbElement : Int =
                    if (store.toLowerCase.contains("url")) NB_URL_RETURNED
                    else if (store.toLowerCase.contains("agent")) NB_AGENT_RETURNED
                    else 0

                  val data = dataCptForDate.dataWithCount.take(nbElement)
                  complete(data)
                }
                case Failure(ex)    => complete(s"An error occurred: ${ex.getMessage}")
              }

          }
        }
      }

    bindingFuture = Http().bindAndHandle(route, hostInfo.host, hostInfo.port)
    println(s"Server online at http://${hostInfo.host}:${hostInfo.port}/\n")

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      bindingFuture
        .flatMap(_.unbind()) // trigger unbinding from the port
        .onComplete(_ => system.terminate()) // and shutdown when done
    }))
  }


//  def fetchRemoteRatingByEmail(host:HostStoreInfo, email: String) : Future[List[String]] = {
//
//    val requestPath = s"http://${hostInfo.host}:${hostInfo.port}/ratingByEmail?email=${email}"
//    println(s"Client attempting to fetch from online at ${requestPath}")
//
//    val responseFuture: Future[List[String]] = {
//      Http().singleRequest(HttpRequest(uri = requestPath))
//        .flatMap(response => Unmarshal(response.entity).to[List[String]])
//    }
//
//    responseFuture
//  }

  def fetchLocalTopUrl(store:String, date: String) : Future[DataCptForDate] = {

    val ec = ExecutionContext.global

    val host = metadataService.streamsMetadataForStoreAndKey[String](
      store,
      date,
      Serdes.String().serializer()
    )

    Retry.retry(5) {
      val f = streams.store(store, QueryableStoreTypes.keyValueStore[String,DataCptForDate]())
      f.get(date)
    }(ec)
  }

  def stop() : Unit = {
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }

  def thisHost(hostStoreInfo: HostStoreInfo) : Boolean = {
    hostStoreInfo.host.equals(hostInfo.host()) &&
      hostStoreInfo.port == hostInfo.port
  }
}
