package org.schtsch.twostreams

import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.Serializable
import scala.util.Try


case class Event(id: String, global_idx: Int, batch: Int) extends Serializable
case class Ref(id: String, idx: Int) extends Serializable

object Main extends App {
  val refDirectory = "data/streaming-ref"
  val dataDirectory = "data/streaming-data"

  val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("TwoSourcesStream")

  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Durations.seconds(5))

  val refStore = BroadcastWrapper(ssc, Map[String, Int]())

  ssc.textFileStream(refDirectory)
    .map(parseRefRec)
    .filter(_ != null)
    .repartition(1)
    .foreachRDD(rdd => {
      val newRefData = rdd
        .map(r => (r.id, r.idx))
        .collectAsMap()

      refStore.update(refStore.value ++ newRefData)
    })

  ssc.textFileStream(dataDirectory)
    .map(parseDataRec)
    .filter(_ != null)
    .map(eventPair => {
      val (id, event) = eventPair
      (id, event.global_idx, event.batch, refStore.value.getOrElse(id, -1))
    })
    .foreachRDD(rdd => {
      rdd.foreach(r => println(r))
    })

  ssc.start()
  ssc.awaitTermination()

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

