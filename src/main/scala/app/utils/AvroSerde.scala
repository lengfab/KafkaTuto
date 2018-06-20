package app.utils

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.util

import com.sksamuel.avro4s._
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}


object AvroSerde{
  val MAGIC_BYTE = 0.toByte

  def keySerde[T : SchemaFor : ToRecord : FromRecord]
    (implicit schemaRegistryClient: SchemaRegistryClient) = new AvroSerde[T](true, schemaRegistryClient)
  def valueSerde[T : SchemaFor : ToRecord : FromRecord]
    (implicit schemaRegistryClient: SchemaRegistryClient) = new AvroSerde[T](false, schemaRegistryClient)
}

class AvroSerde[T: SchemaFor : ToRecord : FromRecord](isKey: Boolean, schemaRegistryClient: SchemaRegistryClient) extends Serde[T]{

  private val (_deserializer, _serializer) = (new AvroDeserializer[T](), new AvroSerializer[T](isKey, schemaRegistryClient))

  override def deserializer(): Deserializer[T] = _deserializer

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ???

  override def close(): Unit = ()

  override def serializer(): Serializer[T] = _serializer
}

class AvroSerializer[T : SchemaFor : ToRecord](isKey: Boolean, schemaRegistryClient: SchemaRegistryClient) extends Serializer[T]{

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: T): Array[Byte] = {
    val schema = implicitly[SchemaFor[T]].apply()
    val schemaId = schemaRegistryClient.register(topic + (if(isKey) "-key" else "-value"), schema)

    val output = new ByteArrayOutputStream()
    val w = new DataOutputStream(output)
    w.writeInt(schemaId)
    w.writeByte(AvroSerde.MAGIC_BYTE)
    w.flush()
    val writer = AvroOutputStream.binary(output)
    writer.write(data)
    writer.flush()
    val retval = output.toByteArray
    writer.close()
    output.close()
    retval
  }

  override def close(): Unit = ()
}

class AvroDeserializer[T : SchemaFor : FromRecord] extends Deserializer[T]{

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): T = AvroInputStream.binary[T](data drop(5)).iterator().toList.head


  override def close(): Unit = ()
}