package com.svend.demo

import java.io.{ByteArrayInputStream, DataInputStream}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.alpakka.slick.scaladsl._
import akka.stream.scaladsl._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
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

object Main extends App {

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
      .via(AvroFlattener.flow(kafkaConfig.getString("key-schema"), kafkaConfig.getString("value-schema")))
      .via(Slick.flow(SqlUtil.asSqlInsert(tableName)))
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

  def flow(keySchemaPath: String, valueSchemaPath: String):
  Flow[ConsumerRecord[Array[Byte], Array[Byte]], Seq[(String, AvroToolkit.Field[Any])], NotUsed] = {

    val keyToolkit = AvroToolkit(keySchemaPath)
    val valueToolkit = AvroToolkit(valueSchemaPath)

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
            keyToolkit.flatten(keyAvro).map { case (fieldName, fieldValue) => (s"kafka_key_$fieldName", fieldValue) }
              ++ valueToolkit.flatten(valueAvro)
              :+ ("_kafka_offset", AvroToolkit.LongField(offset))
              :+ ("_kafka_partition", AvroToolkit.IntField(partition))
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

  val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)

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

  /**
   * Convert this avro record into a sequence of name/value pairs
   */
  def flatten(record: GenericRecord): Seq[(String, AvroToolkit.Field[Any])] = {
    // TODO: actually flattening stuff here
    schema
      .getFields
      .asScala.toSeq
      .map(f => f.name() -> AvroToolkit.Field(f.name().replace(" ", "_"), f.schema(), record))
  }
}


object AvroToolkit {

  def apply(schemaFile: String) = {
    val schema = new Schema.Parser().parse(io.Source.fromFile(schemaFile).getLines().mkString(""))
    new AvroToolkit(schema)
  }

  // Avro field, with one value from the input Generic record
  trait Field[+T] {
    val value: T

    def asSqlParam(pp: PositionedParameters)
  }

  object Field {

    def apply(fieldName: String, schema: Schema, record: GenericRecord): Field[Any] = {
      schema.getType match {

        case Schema.Type.LONG => LongField(record.get(fieldName).asInstanceOf[Long])
        case Schema.Type.INT => IntField(record.get(fieldName).asInstanceOf[Int])
        case Schema.Type.FLOAT => FloatField(record.get(fieldName).asInstanceOf[Float])
        case Schema.Type.DOUBLE => DoubleField(record.get(fieldName).asInstanceOf[Double])
        case Schema.Type.BOOLEAN => BooleanField(record.get(fieldName).asInstanceOf[Boolean])
        case Schema.Type.STRING => StringField(Option(record.get(fieldName)).map(_.asInstanceOf[Utf8].toString).orNull)

        // quick hack: assuming all UNIONS are always structured as [null, actualType]
        case Schema.Type.UNION => this (fieldName, schema.getTypes.get(1), record)

        // TODO: other types + add support for nested avro here
      }
    }
  }

  // conversion from Avro types to Slick SQL types here
  case class LongField(value: Long) extends Field[Long] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setLong(value)
  }

  case class IntField(value: Int) extends Field[Int] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setInt(value)
  }

  case class FloatField(value: Float) extends Field[Float] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setFloat(value)
  }

  case class DoubleField(value: Double) extends Field[Double] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setDouble(value)
  }

  case class BooleanField(value: Boolean) extends Field[Boolean] {
    override def asSqlParam(pp: PositionedParameters): Unit = pp.setBoolean(value)
  }

  case class StringField(value: String) extends Field[String] {
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

  def asSqlInsert(tableName: String)(values: Seq[(String, AvroToolkit.Field[Any])])(implicit slickSession: SlickSession): SqlAction[Int, NoStream, Effect] = {
    import slickSession.profile.api._

    // TODO: strangely enough, I have no idea how to write this more elegantly nor without hardcoding the number of fields... :(

    values.length match {
      case 21 => sqlu"""
          INSERT INTO #$tableName (
          #${values(0)._1} , #${values(1)._1}, #${values(2)._1}, #${values(3)._1}, #${values(4)._1}, #${values(5)._1}, #${values(6)._1}, #${values(7)._1}, #${values(8)._1}, #${values(9)._1},
          #${values(10)._1}, #${values(11)._1}, #${values(12)._1}, #${values(13)._1}, #${values(14)._1}, #${values(15)._1}, #${values(16)._1}, #${values(17)._1}, #${values(18)._1}, #${values(19)._1},
          #${values(20)._1}
          )
          VALUES(
          ${values(0)._2},${values(1)._2},${values(2)._2},${values(3)._2},${values(4)._2},${values(5)._2},${values(6)._2},${values(7)._2},${values(8)._2},${values(9)._2},
          ${values(10)._2},${values(11)._2},${values(12)._2},${values(13)._2},${values(14)._2},${values(15)._2},${values(16)._2},${values(17)._2},${values(18)._2},${values(19)._2},
          ${values(20)._2}
          )"""
    }

  }
}
