name := "akka-quickstart-scala"

version := "1.0"

scalaVersion := "2.13.1"

val akkaVersion = "2.6.3"

libraryDependencies ++= Seq(

  "com.typesafe.akka" %% "akka-stream" % akkaVersion ,
  "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.1",
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "1.1.2",
  "org.postgresql" % "postgresql" % "42.2.5",

  "org.apache.avro" % "avro" % "1.8.2",

  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.1.0" % Test
)
