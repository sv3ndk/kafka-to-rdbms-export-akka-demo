package com.svend.demo

import java.io.{ByteArrayInputStream, DataInputStream}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.alpakka.slick.scaladsl._
import akka.stream.scaladsl._
import com.sksamuel.avro4s.{AvroSchema, RecordFormat, SchemaFor}
import com.svend.demo.DataModel.{Pizza, PizzaId}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import slick.dbio.{Effect, NoStream}
import slick.jdbc.{PositionedParameters, SetParameter}
import slick.sql.SqlAction

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


object IngestDemo extends App {

  implicit val system = ActorSystem("ingestion")

  val appConfig = system.settings.config.getConfig("demo-app")
  val kafkaConfig = appConfig.getConfig("kafka")
  val dbConfig = appConfig.getConfig("db")
  val inputTopic = kafkaConfig.getString("input-topic")

  val tableName = dbConfig.getString("destination-table")

  implicit val ec = system.dispatcher
  implicit val dbSession = SlickSession.forConfig(dbConfig.getConfig("slick"))

  SqlUtil.latestCommittedOffset(tableName).foreach { offsets => {

    println(s"resuming reading kafka from offsets $offsets")

    val rawKafkafRecords = Consumer.plainSource(
      ConsumerSettings(
        kafkaConfig.getConfig("akka-kafka-consumer"),
        new ByteArrayDeserializer, new ByteArrayDeserializer),

      if (offsets.isEmpty) Subscriptions.topicPattern(inputTopic)
      else Subscriptions.assignmentWithOffset(
        offsets.map {
          case (partitionId, offset) => new TopicPartition(inputTopic, partitionId) -> offset
        }.toMap
      )
    )

    rawKafkafRecords
      .via(AvroFlattener.flow(AvroSchema[PizzaId], AvroSchema[Pizza]))
      .via(Slick.flow(SqlUtil.asSqlInsert(tableName)))
      .log("error logging")
      .runWith(Sink.ignore)
  }
  }

  system.registerOnTermination(() => dbSession.close())
}

/**
 * Converts a raw Flow of Avro generic record into a Flow of "flat" sequence of (name -> Field).
 *
 * "Flat" here means that nested fields are "flattened" into a single dimension.
 *
 * All fields of the key are put first, then the fields of the value
 **/
object AvroFlattener {

  def flow(keySchema: Schema, valueSchema: Schema):
  Flow[ConsumerRecord[Array[Byte], Array[Byte]], Seq[AvroToolkit.Field[Any]], NotUsed] = {

    val keyToolkit = new AvroToolkit(keySchema)
    val valueToolkit = new AvroToolkit(valueSchema)

    Flow[ConsumerRecord[Array[Byte], Array[Byte]]]

      // parsing to GenericRecord
      .map(kafkaRecord =>
        for {
          key <- keyToolkit.deserialize(kafkaRecord.key())
          value <- valueToolkit.deserialize(kafkaRecord.value())
        } yield (key, value, kafkaRecord.partition(), kafkaRecord.offset())
      )

      // flattening both keys and values to one single Seq[(String, Field[Any])]
      .map(
        _.map { case (keyAvro, valueAvro, partition, offset) =>
          (
            // fields from the key are prefixed with "key-", to avoid collisions
            AvroToolkit.flatten(keyAvro, "kafka_key_")
              ++ AvroToolkit.flatten(valueAvro)
              :+ AvroToolkit.LongField("_kafka_offset", offset)
              :+ AvroToolkit.IntField("_kafka_partition", partition)
            )
        }
      )

      .mapConcat {
        case Success(fields) => Seq(fields)
        case Failure(exception) => {
          // TODO: some sort of DLQ here...
          println("could not process element => dropping " + exception)
          Seq.empty
        }
      }
  }
}


/**
 * Schema-specific Avro utils method
 **/
class AvroToolkit(val schema: Schema) {

  val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)

  def deserialize(message: Array[Byte]): Try[GenericRecord] = {
    Try {
      val input = new ByteArrayInputStream(message)
      val din = new DataInputStream(input)
      val decoder = DecoderFactory.get.jsonDecoder(schema, din)
      val genericRecord = reader.read(null, decoder)
      din.close()
      genericRecord
    }
  }

}


object AvroToolkit {

  /**
   * Convert this avro record into a sequence of name/value pairs
   */
  def flatten(record: GenericRecord, prefix: String = ""): Seq[AvroToolkit.Field[Any]] = {
    record.getSchema
      .getFields
      .asScala.toSeq
      .flatMap(f => Field(f.name(), f.schema(), record, prefix))
  }

  // Avro field, with one value from the input Generic record
  trait Field[+T] {
    val name: String
    val value: T
    def asSqlParam(pp: PositionedParameters)
  }

  object Field {

    def apply(fieldName: String, schema: Schema, record: GenericRecord, prefix: String = ""): Seq[Field[Any]] = {
      schema.getType match {

        case Schema.Type.LONG => LongField(prefix + fieldName, record.get(fieldName).asInstanceOf[Long]) :: Nil
        case Schema.Type.INT => IntField(prefix + fieldName, record.get(fieldName).asInstanceOf[Int]) :: Nil
        case Schema.Type.FLOAT => FloatField(prefix + fieldName, record.get(fieldName).asInstanceOf[Float]):: Nil
        case Schema.Type.DOUBLE => DoubleField(prefix + fieldName, record.get(fieldName).asInstanceOf[Double]):: Nil
        case Schema.Type.BOOLEAN => BooleanField(prefix + fieldName, record.get(fieldName).asInstanceOf[Boolean]) :: Nil
        case Schema.Type.STRING => StringField(prefix + fieldName, Option(record.get(fieldName)).map(_.asInstanceOf[Utf8].toString).orNull) :: Nil

        // quick hack: assuming all UNIONS are always structured as [null, actualType]
        case Schema.Type.UNION => this (fieldName, schema.getTypes.get(1), record)

        case Schema.Type.RECORD =>
          val subRecord = record.get(fieldName).asInstanceOf[GenericRecord]

          subRecord.getSchema
            .getFields
            .asScala.toSeq
            .flatMap(f => Field(f.name(), f.schema(), subRecord, prefix + fieldName + "_"))

        // NO SUPPORT FOR Avro ARRAY yet

      }
    }
  }

  // conversion from Avro types to Slick SQL types here
  case class LongField(name: String, value: Long) extends Field[Long] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setLong(value)
  }

  case class IntField(name: String, value: Int) extends Field[Int] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setInt(value)
  }

  case class FloatField(name: String, value: Float) extends Field[Float] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setFloat(value)
  }

  case class DoubleField(name: String, value: Double) extends Field[Double] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setDouble(value)
  }

  case class BooleanField(name: String, value: Boolean) extends Field[Boolean] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setBoolean(value)
  }

  case class StringField(name: String, value: String) extends Field[String] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setString(value)
  }

}

object SqlUtil {

  implicit object FieldParameter extends SetParameter[AvroToolkit.Field[Any]] {
    override def apply(v: AvroToolkit.Field[Any], pp: PositionedParameters): Unit = v.asSqlParam(pp)
  }

  /**
   * Discover the latest written offset from the destination table
   */
  def latestCommittedOffset(tableName: String)(implicit slickSession: SlickSession): Future[Vector[(Int, Long)]] = {

    import slickSession.profile.api._

    slickSession.db.run(
      sql"""
          select _kafka_partition, max(_kafka_offset)
          from #$tableName
          group by _kafka_partition
         """.as[(Int, Long)]
    )
  }

  def asSqlInsert(tableName: String)(fields: Seq[AvroToolkit.Field[Any]])(implicit slickSession: SlickSession): SqlAction[Int, NoStream, Effect] = {
    import slickSession.profile.api._

    // TODO: strangely enough, I have no idea how to write this more elegantly nor without hardcoding the number of fields... :(

    fields.length match {

      case 7 =>
        sqlu"""
           INSERT INTO #$tableName
          (#${fields(0).name} , #${fields(1).name}, #${fields(2).name}, #${fields(3).name}, #${fields(4).name}, #${fields(5).name}, #${fields(6).name})
          VALUES(${fields(0)},${fields(1)},${fields(2)},${fields(3)},${fields(4)},${fields(5)},${fields(6)})
          """

      case 9 =>
        sqlu"""
           INSERT INTO #$tableName
          (#${fields(0).name} , #${fields(1).name}, #${fields(2).name}, #${fields(3).name}, #${fields(4).name}, #${fields(5).name}, #${fields(6).name}, #${fields(7).name}, #${fields(8).name})
          VALUES(${fields(0)},${fields(1)},${fields(2)},${fields(3)},${fields(4)},${fields(5)},${fields(6)},${fields(7)},${fields(8)})
          """

      case 21 => sqlu"""
          INSERT INTO #$tableName (
          #${fields(0).name} , #${fields(1).name}, #${fields(2).name}, #${fields(3).name}, #${fields(4).name}, #${fields(5).name}, #${fields(6).name}, #${fields(7).name}, #${fields(8).name}, #${fields(9).name},
          #${fields(10).name}, #${fields(11).name}, #${fields(12).name}, #${fields(13).name}, #${fields(14).name}, #${fields(15).name}, #${fields(16).name}, #${fields(17).name}, #${fields(18).name}, #${fields(19).name},
          #${fields(20).name}
          )
          VALUES(
          ${fields(0)},${fields(1)},${fields(2)},${fields(3)},${fields(4)},${fields(5)},${fields(6)},${fields(7)},${fields(8)},${fields(9)},
          ${fields(10)},${fields(11)},${fields(12)},${fields(13)},${fields(14)},${fields(15)},${fields(16)},${fields(17)},${fields(18)},${fields(19)},
          ${fields(20)}
          )"""
    }

  }
}
