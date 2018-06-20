package app.utils

import java.time.LocalDate
import java.util

import com.twitter.chill.KryoInjection
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.util.Try

/**
  * Created by fred on 19/04/2017.
  */


class LocalDateKryoSerializer extends Serializer[LocalDate] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: LocalDate): Array[Byte] = {
    import java.nio.ByteBuffer
    val buffer: ByteBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.putLong(data.toEpochDay)
    buffer.array()
  }


  override def close(): Unit = {}
}


class LocalDateKryoDeserializer extends Deserializer[LocalDate] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): LocalDate = {

    import java.nio.ByteBuffer
    val buffer: ByteBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.put(data)
    buffer.flip //need flip
    LocalDate.ofEpochDay(buffer.getLong)

  }
}


class LocalDateKryoSerde extends Serde[LocalDate] {

  override def deserializer(): Deserializer[LocalDate] = new LocalDateKryoDeserializer()

  override def serializer(): Serializer[LocalDate] = new LocalDateKryoSerializer()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}


class CaseClassKryoSerializer[T] extends Serializer[T] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: T): Array[Byte] = {

    val apply = KryoInjection.apply(data)

    apply
  }

  override def close(): Unit = {}
}


class CaseClassKryoDeserializer[T] extends Deserializer[T] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, bytes: Array[Byte]): T = {

    if(bytes != null ) {
      val invert: Try[Object] = KryoInjection.invert(bytes)
      invert.get.asInstanceOf[T] // we want it to fail
    }
    else{
      null.asInstanceOf[T]
    }
  }

}


class ListKryoSerDe[T ] extends Serde[List[T]] {
  override def deserializer(): Deserializer[List[T]] = new CaseClassKryoDeserializer[List[T]]

  override def serializer(): Serializer[List[T]] = new CaseClassKryoSerializer[List[T]]

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}


class CaseClassKryoSerDe[T <: Product] extends Serde[T] {
  override def deserializer(): Deserializer[T] = new CaseClassKryoDeserializer[T]

  override def serializer(): Serializer[T] = new CaseClassKryoSerializer[T]

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}


class LongKryoSerDe extends Serde[java.lang.Long] {
  override def deserializer(): Deserializer[java.lang.Long] = new CaseClassKryoDeserializer[java.lang.Long]

  override def serializer(): Serializer[java.lang.Long] = new CaseClassKryoSerializer[java.lang.Long]

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}

class BooleanKryoSerDe extends Serde[Boolean] {
  override def deserializer(): Deserializer[Boolean] = new CaseClassKryoDeserializer[Boolean]

  override def serializer(): Serializer[Boolean] = new CaseClassKryoSerializer[Boolean]

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}
