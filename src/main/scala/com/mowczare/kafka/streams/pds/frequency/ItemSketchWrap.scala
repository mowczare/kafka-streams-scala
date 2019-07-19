package com.mowczare.kafka.streams.pds.frequency

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.yahooIntegration.serialization.YahooGenCodecs
import com.yahoo.sketches.frequencies.ItemsSketch

import scala.reflect.ClassTag

//TODO
case class ItemSketchWrap[T](itemSketch: ItemsSketch[T]) {

  def add(elem: T): ItemSketchWrap[T] = {
    itemSketch.update(elem)
    this
  }

  def add(elem: T, count: Long): ItemSketchWrap[T] = {
    itemSketch.update(elem, count)
    this
  }
}

object ItemSketchWrap {
  import YahooGenCodecs._

  def empty[T](size: Int): ItemSketchWrap[T] = ItemSketchWrap(new ItemsSketch[T](size)) //some capacity move it to api in the future

  implicit def genCodec[T : GenCodec : ClassTag]: GenCodec[ItemSketchWrap[T]] =
    GenCodec.transformed[ItemSketchWrap[T], ItemsSketch[T]](
    _.itemSketch,
    ItemSketchWrap(_)
  )
}


