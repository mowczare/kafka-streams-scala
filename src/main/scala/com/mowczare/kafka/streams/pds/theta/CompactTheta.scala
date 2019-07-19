package com.mowczare.kafka.streams.pds.theta

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.model.UniqueCount
import com.mowczare.kafka.streams.pds.yahooIntegration.serialization.YahooGenCodecs
import com.yahoo.sketches.theta.CompactSketch

class CompactTheta(private[theta] val internal: CompactSketch) {
  def estimate: Double = internal.getEstimate
  def uniqueCountEstimate: UniqueCount = UniqueCount(internal.getEstimate.round)
}

object CompactTheta extends YahooGenCodecs {

  implicit val genCodec: GenCodec[CompactTheta] =
    GenCodec.transformed[CompactTheta, CompactSketch](
      _.internal,
      new CompactTheta(_)
    )

}
