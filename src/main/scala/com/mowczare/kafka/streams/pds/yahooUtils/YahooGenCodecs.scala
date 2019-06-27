package com.mowczare.kafka.streams.pds.yahooUtils

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.yahooUtils.serialization.{DataSerdeScalaWrap, GencodecArrayOfItemsSerDe}
import com.yahoo.memory.Memory
import com.yahoo.sketches.frequencies.ItemsSketch
import com.yahoo.sketches.hll.HllSketch
import com.yahoo.sketches.theta.{Sketches, UpdateSketch}

import scala.reflect.ClassTag

trait YahooGenCodecs {

  implicit val hllGenCodec: GenCodec[HllSketch] =
    GenCodec.transformed[HllSketch, Array[Byte]](_.toCompactByteArray, HllSketch.heapify)

  implicit def itemSketchGencodec[T: GenCodec : ClassTag]: GenCodec[ItemsSketch[T]] =
    GenCodec.transformed[ItemsSketch[T], Array[Byte]](_.toByteArray(new DataSerdeScalaWrap(new GencodecArrayOfItemsSerDe[T]())),
      byteArray =>
        ItemsSketch.getInstance(Memory.wrap(byteArray), new DataSerdeScalaWrap(new GencodecArrayOfItemsSerDe[T]()))
    )

  implicit val thetaGenCodec: GenCodec[UpdateSketch] =
    GenCodec.transformed[UpdateSketch, Array[Byte]](
      _.toByteArray,
      byteArray => Sketches.heapifyUpdateSketch(Memory.wrap(byteArray))
    )
}

object YahooGenCodecs extends YahooGenCodecs
