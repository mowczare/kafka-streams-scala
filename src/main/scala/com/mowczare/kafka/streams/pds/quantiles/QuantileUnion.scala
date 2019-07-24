package com.mowczare.kafka.streams.pds.quantiles

import java.util.Comparator

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.yahooIntegration.serialization.YahooGenCodecs
import com.yahoo.sketches.quantiles.ItemsUnion

import scala.reflect.ClassTag

class QuantileUnion[T: ClassTag](val internal: ItemsUnion[T]) {

  def add(elem: T): QuantileUnion[T] = {
    internal.update(elem)
    this
  }

  def union(quantile: Quantile[T]): QuantileUnion[T] = {
    internal.update(quantile.internal)
    this
  }

  def result: Quantile[T] = new Quantile[T](internal.getResult)

}
object QuantileUnion extends YahooGenCodecs {
  def empty[T: Ordering: ClassTag](quantilesNum: Int): QuantileUnion[T] = {
    new QuantileUnion(
      ItemsUnion.getInstance(quantilesNum, implicitly[Comparator[T]])
    )
  }

  implicit def genCodec[T: ClassTag: Ordering: GenCodec]
    : GenCodec[QuantileUnion[T]] =
    GenCodec.transformed[QuantileUnion[T], ItemsUnion[T]](
      _.internal,
      new QuantileUnion(_)
    )

}