package com.mowczare.kafka.streams.pds.yahooIntegration.serialization

import com.avsystem.commons.serialization.GenCodec
import com.yahoo.memory.Memory
import com.yahoo.sketches.frequencies.ItemsSketch
import com.yahoo.sketches.hll.{HllSketch, Union => HllUnion}
import com.yahoo.sketches.theta.{SetOperation, Sketch, Sketches, UpdateSketch, CompactSketch => ThetaCompactSketch, Union => ThetaUnion}

import scala.reflect.ClassTag

trait YahooGenCodecs {

  implicit val hllSketchGenCodec: GenCodec[HllSketch] =
    GenCodec.transformed[HllSketch, Array[Byte]](
      _.toCompactByteArray,
      HllSketch.heapify
    )

  implicit val hllUnionGenCodec: GenCodec[HllUnion] =
    GenCodec.transformed[HllUnion, Array[Byte]](
      _.toCompactByteArray,
      HllUnion.heapify
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

  implicit val thetaUnionGenCodec: GenCodec[ThetaUnion] =
    GenCodec.transformed[ThetaUnion, Array[Byte]](
      _.toByteArray,
      byteArray => SetOperation.heapify(Memory.wrap(byteArray)).asInstanceOf[ThetaUnion]
    )

  implicit val sketchCodec: GenCodec[Sketch] = {
    GenCodec.transformed[Sketch, Array[Byte]](
      _.toByteArray,
      byteArray => Sketch.heapify(Memory.wrap(byteArray))
    )
  }

  implicit val compactSketch: GenCodec[ThetaCompactSketch] =
    GenCodec.transformed[ThetaCompactSketch, Sketch](
      identity,
      sketch => sketch.asInstanceOf[ThetaCompactSketch]
    )
}

object YahooGenCodecs extends YahooGenCodecs