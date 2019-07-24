package com.mowczare.kafka.streams.pds.frequency

import com.avsystem.commons.serialization.json.{JsonStringInput, JsonStringOutput}
import com.mowczare.kafka.streams.example.model.InputEvent
import org.scalatest.FunSuite

class ItemSketchWrapTest extends FunSuite {

  def inputEvent(): InputEvent = {
    InputEvent(-1)
  }


  test("testGenCodec") {
    val xd = ItemSketchWrap.empty[InputEvent](64)
    (1 to 2).foreach(_ => xd.add(inputEvent))

    val str = JsonStringOutput.write(xd)
    val finalValue = JsonStringInput.read[ItemSketchWrap[InputEvent]](str)
    println(finalValue.itemSketch.getStreamLength)
  }

}
