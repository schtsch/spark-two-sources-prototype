package org.schtsch.twostreams

import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.Serializable
import scala.util.Try


case class Event(id: String, global_idx: Int, batch: Int) extends Serializable
case class Ref(id: String, idx: Int) extends Serializable
case class Entity(id: String, events: Seq[Event], ref: Option[Ref]) extends Serializable


object Main extends App {
  val refDirectory = "data/streaming-ref"
  val dataDirectory = "data/streaming-data"

  val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("TwoSourcesStream")

  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Durations.seconds(5))
  ssc.checkpoint("data/checkpoints")

  val refStream = ssc.textFileStream(refDirectory)
    .map(parseRefRec)
    .filter(_ != null)
    .map(r => (r.id, r))

  ssc.textFileStream(dataDirectory)
    .map(parseDataRec)
    .filter(_ != null)
    .groupByKey()
    .fullOuterJoin(refStream)
    .updateStateByKey(updateEntity)
    .repartition(1)
    .foreachRDD(rdd => {
      rdd.foreach(r => println(r._2))
    })

  ssc.start()
  ssc.awaitTermination()

  def updateEntity(evtRefPair: Seq[(Option[Iterable[Event]], Option[Ref])], entity: Option[Entity]): Option[Entity] = {
    val events = evtRefPair.flatMap(_._1).flatten
    val ref = evtRefPair.flatMap(_._2).lastOption
    entity match {
      case Some(entity) => Some(entity.copy(events = entity.events ++ events, ref =  if (ref.isEmpty) entity.ref else ref))
      case None => Some(Entity(if (events.nonEmpty) events.head.id else ref.get.id, events, ref))
    }
  }

  def parseDataRec(line: String): (String, Event) = {
      line.split(',') match {
        case Array(id, idx, batch) =>
          Try(Event(id, Integer.parseInt(idx), Integer.parseInt(batch)))
            .map(evt => (evt.id, evt))
            .getOrElse(null)
        case _ =>
          println(s"can't parse line: $line")
          null
      }
  }

  def parseRefRec(line: String): Ref = {
    line.split(',') match {
      case Array(id, idx) =>
        Try(Ref(id, Integer.parseInt(idx))).getOrElse(null)
      case _ =>
        println(s"can't parse line: $line")
        null
    }
  }
}

