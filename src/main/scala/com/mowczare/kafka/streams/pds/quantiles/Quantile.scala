package com.mowczare.kafka.streams.pds.quantiles

import java.util.Comparator

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.yahooIntegration.serialization.YahooGenCodecs
import com.yahoo.sketches.quantiles.ItemsSketch

import scala.reflect.ClassTag

class Quantile[T : ClassTag](private [quantiles] val internal: ItemsSketch[T]) {

  def add(item: T): Quantile[T] = {
    internal.update(item)
    this
  }

  def pmf(splitPoints: IndexedSeq[T]): IndexedSeq[Double] =
    internal.getPMF(splitPoints.toArray.asInstanceOf[Array[T with Object]]) //TODO

  def downSampled(quantilesNum: Int): Quantile[T] = {
    new Quantile(internal.downSample(quantilesNum))
  }
}

object Quantile extends YahooGenCodecs {

  def empty[T : Ordering](quantilesNum: Int): ItemsSketch[T] = {
    ItemsSketch.getInstance(quantilesNum, implicitly[Comparator[T]])
  }

  implicit def genCodec[T : ClassTag : Ordering : GenCodec]: GenCodec[Quantile[T]] =
    GenCodec.transformed[Quantile[T], ItemsSketch[T]](
      _.internal,
      new Quantile(_)
    )

}
