package com.mowczare.kafka.streams.pds.quantiles

import java.util.Comparator

import com.yahoo.sketches.quantiles.ItemsSketch

//TODO
class Quantile[T](private [quantiles] val internal: ItemsSketch[T]) {

  def add(item: T): Quantile[T] = {
    internal.update(item)
    this
  }
  
}
object Quantile {

  def empty[T](quantilesNum: Int)(implicit ordering: Ordering[T]): ItemsSketch[T] = {
    ItemsSketch.getInstance(quantilesNum, implicitly[Comparator[T]])
  }

}
