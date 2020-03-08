package com.svend.demo

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import com.sksamuel.avro4s.JsonFormat
import com.sksamuel.avro4s.kafka.GenericSerde
import org.apache.kafka.clients.producer.ProducerRecord
import com.svend.demo.DataModel._

import scala.concurrent.duration._

/**
 * Quick and dirty app to send sample avro data to Kafka
 */
object DataGeneratorApp extends App {

  implicit val system = ActorSystem("data-generator")

  val appConfig = system.settings.config.getConfig("data-generator")
  val outputTopic = appConfig.getString("output-topic")

  val kakaConfig = ProducerSettings(
    appConfig.getConfig("kafka-producer"),
    new GenericSerde[PizzaId](JsonFormat),
    new GenericSerde[Pizza](JsonFormat)
  )

  println("Starting to generate hard-coded data")

  DataGenerator.dataSource
    .throttle(1, 1.second)
    .map{ case (id, pizza) => new ProducerRecord[PizzaId, Pizza]( outputTopic, id, pizza)}
//    .runForeach(println)
    .runWith(Producer.plainSink(kakaConfig))

}


object DataGenerator {

  def dataSource: Source[(PizzaId, Pizza), NotUsed] = Source.unfold(0)(onePizza)

  // generates one pizza to be sent to kafka
  def onePizza(index: Int): Option[(Int, (PizzaId, Pizza))] = {
    Some((
      index+1,
      (PizzaId(index), Pizza("Pandemic pizza", true, false, 27, Person("Jean-Marc", "OfOven")))
    ))
  }

}
