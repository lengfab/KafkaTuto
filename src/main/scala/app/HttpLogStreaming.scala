/**
  * Copyright 2016 Confluent Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
  * in compliance with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License
  * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
  * or implied. See the License for the specific language governing permissions and limitations under
  * the License.
  */

package app

import java.text.SimpleDateFormat
import java.util.{Collections, Locale, Properties, Date}

import app.restService.TopRestService
import app.utils._
import com.sksamuel.avro4s.{AvroSchema, FromRecord, SchemaFor, ToRecord}
import fr.psug.kafka.streams.KafkaStreamsImplicits._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDecoder}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroSerde, SpecificAvroSerde}
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Base64
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.rogach.scallop._

import scala.language.implicitConversions
import scala.util.matching.Regex


case class UrlData(url:String, agent:String, count:Long, date: Long)


case class DataWithDate(data:String, count:Long, date: Long)
case class DataCpt(data: String, count: Long)
case class DataCptForDate(date: Long, dataWithCount: Seq[DataCpt])

object HttpLogStreaming {

  val agentReg : Regex = "(MSIE|(?!Gecko.+)Firefox|(?!AppleWebKit.+Chrome.+)Safari|(?!AppleWebKit.+)Chrome|AppleWebKit(?!.+Chrome|.+Safari)|Gecko(?!.+Firefox))(?: |\\/)([\\d\\.apre]+)".r

  val stringSerde = Serdes.String()
  val dataCptForDateSchema = AvroSchema[DataCptForDate]
  val dataCptForDateSerde = new CaseClassKryoSerDe[DataCptForDate]
  val dataDataSerde = new CaseClassKryoSerDe[DataWithDate]



  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val bootstrap = opt[String](required = true)
    val schema_registry = opt[String](required = true)
    val topic = opt[String](required = true)
    val zkurl = opt[String](required = true)
    verify()
  }

  val format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  def getDay(time: String) : Date = {
    format.parse(time)
  }

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    val bootstrapServers : String = conf.bootstrap()
    val schemaRegistryUrl : String = conf.schema_registry()
    val topic : String = conf.topic()
    val zkurl : String = conf.zkurl()

    val builder: StreamsBuilder = new StreamsBuilder()
    //val dataCptForDateSerde = AvroSerde.valueSerde[DataCptForDate](schemaRegistryUrl) //avro serde for kafka connect

    val streamingConfig = {
      val settings = new Properties
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-scala-example")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
      settings
    }

    // Read the input Kafka topic into a KStream instance.
    val httplogStream: KStream[String, String] = builder.stream(topic)

    val props = new Properties
    props.put("schema.registry.url", schemaRegistryUrl)
    val valueDecoder =  new KafkaAvroDecoder(new VerifiableProperties(props))

    // logstash send the avro data in base 64
    val recordsStream: KStream[String, GenericRecord] = httplogStream.mapValues(payload => {
      val data = Base64.decoder().decode(payload)
      valueDecoder.fromBytes(data).asInstanceOf[GenericRecord]
    })

    import io.confluent.examples.streams.KeyValueImplicits._

    val urlDataStream: KStream[String, UrlData] = recordsStream.map((key, value) => {
      //println("value="+value)
      val day : Date = value.get("timestamp") match {
        case timestamp : Utf8 => getDay(timestamp.toString)
        case _ => new Date()
      }
      val url : String = value.get("referrer") match {
        case "-" => {
          value.get("request") match {
            case request: Utf8 => request.toString
            case _ => ""
          }
        }
        case referrer : Utf8 => referrer.toString
        case _ => ""
      }
      val agent : String = value.get("agent") match {
        case agent : Utf8 =>
          agentReg.findFirstMatchIn(agent.toString) match {
          case Some(m) => m.group(0)
          case _ => {
            println("no agent found : " + agent.toString)
            "Unkown agent"
          }
        }
        case _ => "Unkown agent"
      }
      val dateKey = Utils.getKey(day)
      (dateKey.toString, UrlData(url, agent, 1L, dateKey))
    })
    //urlDataStream.print()

    //reducer to reduce each url for one date
    def reducer1(left: DataWithDate, right : DataWithDate) : DataWithDate = DataWithDate(left.data, left.count + right.count, left.date)
    //reducer to reduce all the url for one date
    def reducer2(left: DataCptForDate, right : DataCptForDate) : DataCptForDate = {
      println(s"left = $left  right=$right")
      val unionSeq = right.dataWithCount.union(left.dataWithCount)

      val seqUrlCpt: Seq[DataCpt] = unionSeq.foldLeft(Seq.empty[DataCpt]){ (acc, v) =>
        val urls = acc.map(v => v.data)
        val filteredValue = acc.filter(_.data == v.data)
        if (filteredValue.isEmpty)
          acc :+ v
        else if (v.count > filteredValue.head.count )
          acc :+ v
        else
          acc
      }

      val top10Url = seqUrlCpt.sortBy(- _.count).take(10)
      DataCptForDate(right.date, top10Url)
    }

    implicit val schemafor = SchemaFor[DataCptForDate]
    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
    val dataSerde = AvroSerde.valueSerde[DataCptForDate](schemafor, ToRecord[DataCptForDate], FromRecord[DataCptForDate], schemaRegistryClient)

    def reduceData(stream: KStream[String, DataWithDate], name:String) = {
      stream
        .typesafe
//              .map{ (k,v) =>
//                println(s"topUrlStream : k : $k, v : $v")
//                (k,v)
//              }
        .groupByKey(stringSerde, dataDataSerde)
        .reduce(reducer1, "Table_inter_" + name)
        .toStream
        .typesafe
        .map { (k, v) =>
          //println(s"table1 : k : $k, v : $v")
          (v.date.toString, DataCptForDate(v.date, Seq(DataCpt(v.data, v.count))))
        }
        .groupByKey(stringSerde, dataCptForDateSerde)
        .reduce(reducer2, name+"_store")
        .toStream
        .typesafe
        .map{ (k,v) =>
          println(s"topTable $name : k : $k, v : $v")
          (k,v)
        }
        .to(name)(stringSerde, dataSerde)
    }

    //reduce the top url
    val topUrlStream : KStream[String, DataWithDate]= urlDataStream
      .map{(k, v) => (k + '_' + v.url, DataWithDate(v.url, v.count, v.date))}

    val topUrlTopicName = "topUrl"
    TopicAdmin.createTopic(zkurl, Topic(topUrlTopicName, 1, 1))
    reduceData(topUrlStream, topUrlTopicName)

    //reduce the top browser
    val topAgentStream : KStream[String, DataWithDate]= urlDataStream
      .map{(k, v) => (k + '_' + v.agent, DataWithDate(v.agent, v.count, v.date))}
    val topAgentTopicName = "topAgent"
    TopicAdmin.createTopic(zkurl, Topic(topAgentTopicName, 1, 1))
    reduceData(topAgentStream, topAgentTopicName)

    //topUrlTable.print()

//    val producedProps = new java.util.HashMap[String, String]()
//    producedProps.put("schema.registry.url", schemaRegistryUrl)
//    val genericRecordSerde = new GenericAvroSerde()
//    genericRecordSerde.configure(producedProps, false)
//    recordsStream.to("outputHttplog", Produced.`with`(Serdes.String(), genericRecordSerde))

    val stream: KafkaStreams = new KafkaStreams(builder.build(), streamingConfig)
    stream.start()

    //start the rest service
    new TopRestService(stream, new HostInfo("localhost", 9000)).start()
  }
}