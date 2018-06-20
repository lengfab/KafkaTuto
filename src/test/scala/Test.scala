package app

import java.io.ByteArrayInputStream
import java.util.Properties

import app.HttpLogStreaming.agentReg
import io.confluent.kafka.serializers.{AbstractKafkaAvroDeserializer, KafkaAvroDecoder}
import kafka.utils.VerifiableProperties
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.util.Utf8
import org.apache.kafka.common.utils.Base64
;

object Test extends App {

  import java.io.ByteArrayOutputStream

  import org.apache.avro.Schema
  import org.apache.avro.Schema.Parser
  import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
  import org.apache.avro.io.EncoderFactory


  val jsonSchema = "{\"type\":\"record\",\"name\":\"HttpLog\",\"namespace\":\"<empty>\",\"fields\":[{\"name\":\"agent\",\"type\":\"string\"},{\"name\":\"auth\",\"type\":\"string\"},{\"name\":\"ident\",\"type\":\"string\"},{\"name\":\"verb\",\"type\":\"string\"},{\"name\":\"message\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"response\",\"type\":\"int\"},{\"name\":\"bytes\",\"type\":\"int\"},{\"name\":\"clientip\",\"type\":\"string\"},{\"name\":\"httpversion\",\"type\":\"string\"}]}"
  val schema : Schema = (new Parser()).parse(jsonSchema)

  val record : GenericRecord = new GenericData.Record(schema)
  record.put("agent", "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:25.0) Gecko/20100101 Firefox/25.0\"")
  record.put("auth", "-")
  record.put("ident", "-")
  record.put("verb", "GET")
  record.put("message", "127.0.0.1 - - [11/Dec/2013:00:01:45 -0800] \"GET /xampp/status.php HTTP/1.1\" 200 3891 \"http://cadenza/xampp/navi.php\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:25.0) Gecko/20100101 Firefox/25.0\"")
  record.put("response",200)
  record.put("bytes", 3891)
  record.put("clientip", "127.0.0.1")
  record.put("httpversion", "1.1")
  record.put("timestamp", "11/Dec/2013:00:01:45 -0800")

  println("record = " + record)

  val writer = new GenericDatumWriter[GenericRecord](schema)
  val out = new ByteArrayOutputStream()
  val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
  writer.write(record, encoder)
  encoder.flush() // !
  out.toByteArray.foreach(print)

  val json = "{\"agent\":\"\\\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:25.0) Gecko/20100101 Firefox/25.0\\\"\",\"auth\":\"-\",\"ident\":\"-\",\"verb\":\"GET\",\"message\":\"127.0.0.1 - - [11/Dec/2013:00:01:45 -0800] \\\"GET /xampp/status.php HTTP/1.1\\\" 200 3891 \\\"http://cadenza/xampp/navi.php\\\" \\\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:25.0) Gecko/20100101 Firefox/25.0\\\"\",\"response\":200,\"bytes\":3891,\"clientip\":\"127.0.0.1\",\"@version\":\"1\",\"httpversion\":\"1.1\",\"timestamp\":\"11/Dec/2013:00:01:45 -0800\"}"


  val payload = "AAAAAAGmASJNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNS4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI1LjAiAi0CLQZHRVSQAzEyNy4wLjAuMSAtIC0gWzExL0RlYy8yMDEzOjAwOjAxOjQ1IC0wODAwXSAiR0VUIC94YW1wcC9zdGF0dXMucGhwIEhUVFAvMS4xIiAyMDAgMzg5MSAiaHR0cDovL2NhZGVuemEveGFtcHAvbmF2aS5waHAiICJNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNS4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI1LjAiNDExL0RlYy8yMDEzOjAwOjAxOjQ1IC0wODAwkAPmPBIxMjcuMC4wLjEGMS4x"
  val data = Base64.decoder().decode(payload)
  println()
  println(new String(data))

  val inputData = data.slice(5, data.length)
  val reader = new GenericDatumReader[GenericRecord](schema)
  val in = new ByteArrayInputStream(inputData)
  val decoder = DecoderFactory.get().binaryDecoder(in, null)
  val result : GenericRecord = reader.read(null, decoder)
  println("result = " + result)

  val sourcePath: String = "/tmp/hdfs-watcher-test/test/staging/pending/random/8d63d94c-1e3e-4a54-8f70-015d8368bedc"

  val env = "test"
  val regex = """[S+]?\/"""+env+"""/(\S+)"""
  val r = regex.r
  val res = r.findAllIn(sourcePath)
  println(res.group(1))
}
