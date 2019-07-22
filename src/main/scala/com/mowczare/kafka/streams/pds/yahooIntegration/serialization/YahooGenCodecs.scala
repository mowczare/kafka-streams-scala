package com.mowczare.kafka.streams.pds.yahooIntegration.serialization

import java.util.Comparator

import com.avsystem.commons.serialization.GenCodec
import com.yahoo.memory.Memory
import com.yahoo.sketches.frequencies.{ItemsSketch => FItemsSketch}
import com.yahoo.sketches.quantiles.{ItemsSketch => QItemsSketch}
import com.yahoo.sketches.quantiles.{ItemsUnion => QItemsUnion}
import com.yahoo.sketches.hll.{HllSketch, Union => HllUnion}
import com.yahoo.sketches.theta.{
  SetOperation,
  Sketch,
  Sketches,
  UpdateSketch,
  CompactSketch => ThetaCompactSketch,
  Union => ThetaUnion
}

import scala.math.Ordering._
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

  implicit def fItemSketchGencodec[T: GenCodec: ClassTag]
    : GenCodec[FItemsSketch[T]] =
    GenCodec.transformed[FItemsSketch[T], Array[Byte]](
      _.toByteArray(
        ArrayOfItemsSerDeOps.arrayOfItemsSerDe
      ),
      byteArray =>
        FItemsSketch.getInstance(
          Memory.wrap(byteArray),
          ArrayOfItemsSerDeOps.arrayOfItemsSerDe
        )
    )

  implicit def fItemsUnionGencodec[T: GenCodec: ClassTag]
  : GenCodec[FItemsSketch[T]] =
    GenCodec.transformed[FItemsSketch[T], Array[Byte]](
      _.toByteArray(
        ArrayOfItemsSerDeOps.arrayOfItemsSerDe
      ),
      byteArray =>
        FItemsSketch.getInstance(
          Memory.wrap(byteArray),
          ArrayOfItemsSerDeOps.arrayOfItemsSerDe
        )
    )

  implicit def qItemSketchGencodec[T: GenCodec: ClassTag: Ordering]
    : GenCodec[QItemsSketch[T]] =
    GenCodec.transformed[QItemsSketch[T], Array[Byte]](
      _.toByteArray(
        ArrayOfItemsSerDeOps.arrayOfItemsSerDe
      ),
      byteArray =>
        QItemsSketch.getInstance(
          Memory.wrap(byteArray),
          implicitly[Comparator[T]],
          ArrayOfItemsSerDeOps.arrayOfItemsSerDe
        )
    )

  implicit def qItemUnionGencodec[T: GenCodec: ClassTag: Ordering]
  : GenCodec[QItemsUnion[T]] =
    GenCodec.transformed[QItemsUnion[T], Array[Byte]](
      _.toByteArray(
        ArrayOfItemsSerDeOps.arrayOfItemsSerDe
      ),
      byteArray =>
        QItemsUnion.getInstance(
          Memory.wrap(byteArray),
          implicitly[Comparator[T]],
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
      byteArray =>
        SetOperation.heapify(Memory.wrap(byteArray)).asInstanceOf[ThetaUnion]
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
