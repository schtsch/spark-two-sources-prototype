package org.schtsch.twostreams

import org.apache.spark.streaming.StreamingContext

import java.io.Serializable
import scala.reflect.ClassTag

case class BroadcastWrapper[T: ClassTag](
    @transient private val ssc: StreamingContext,
    @transient private val _v: T
  ) extends Serializable {
  @transient private var v = ssc.sparkContext.broadcast(_v)

  def value: T = v.value

  def update(newValue: T, blocking: Boolean = false): Unit = {
    v.unpersist(blocking)
    v = ssc.sparkContext.broadcast(newValue)
  }
}
