package com.svend.demo

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{Decoder, Encoder}
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema, SchemaFor}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

// essentially a copy-paste of the avro4s Generic Serde, with optional support for JSON
// https://github.com/sksamuel/avro4s/blob/master/avro4s-kafka/src/main/scala/com/sksamuel/avro4s/kafka/GenericSerde.scala
class Avro4sSerdes [T >: Null : SchemaFor : Encoder : Decoder](jsonFormat: Boolean = false) extends Serde[T]
  with Deserializer[T]
  with Serializer[T]
  with Serializable {

  val schema: Schema = AvroSchema[T]

  override def serializer(): Serializer[T] = this

  override def deserializer(): Deserializer[T] = this

  override def deserialize(topic: String, data: Array[Byte]): T = {
    if (data == null) null else {
      val avroInputStream = if (jsonFormat) AvroInputStream.json[T] else AvroInputStream.binary[T]
      val input = avroInputStream.from(data).build(schema)
      val result = input.iterator.next()
      input.close()
      result
    }
  }

  override def close(): Unit = ()

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val avroOutputStream = if (jsonFormat) AvroOutputStream.json[T] else AvroOutputStream.binary[T]
    val output = avroOutputStream.to(baos).build(schema)
    output.write(data)
    output.close()
    baos.toByteArray
  }
}
