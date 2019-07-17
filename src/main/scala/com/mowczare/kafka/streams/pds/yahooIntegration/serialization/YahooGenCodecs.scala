package com.mowczare.kafka.streams.pds.yahooIntegration.serialization

import com.avsystem.commons.serialization.GenCodec
import com.yahoo.memory.Memory
import com.yahoo.sketches.frequencies.ItemsSketch
import com.yahoo.sketches.hll.HllSketch
import com.yahoo.sketches.theta.{Sketches, UpdateSketch}

import scala.reflect.ClassTag

trait YahooGenCodecs {

  implicit val hllSketchGenCodec: GenCodec[HllSketch] =
    GenCodec.transformed[HllSketch, Array[Byte]](
      _.toCompactByteArray,
      HllSketch.heapify
    )

  implicit def itemSketchGencodec[T: GenCodec: ClassTag]
    : GenCodec[ItemsSketch[T]] =
    GenCodec.transformed[ItemsSketch[T], Array[Byte]](
      _.toByteArray(
        ArrayOfItemsSerDeOps.arrayOfItemsSerDe
      ),
      byteArray =>
        ItemsSketch.getInstance(
          Memory.wrap(byteArray),
          ArrayOfItemsSerDeOps.arrayOfItemsSerDe
        )
    )

  implicit val updateSketchGenCodec: GenCodec[UpdateSketch] =
    GenCodec.transformed[UpdateSketch, Array[Byte]](
      _.toByteArray,
      byteArray => Sketches.heapifyUpdateSketch(Memory.wrap(byteArray))
    )
}

object YahooGenCodecs extends YahooGenCodecs