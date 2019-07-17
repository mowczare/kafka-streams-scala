package com.mowczare.kafka.streams.pds.hashing

import java.nio.charset.Charset

import com.avsystem.commons.serialization.{ListOutput, ObjectOutput, Output, SimpleOutput}
import com.google.common.hash.PrimitiveSink

class PrimitiveSinkOutput(primitiveSink: PrimitiveSink) extends Output {

  override def writeNull(): Unit = {}

  override def writeSimple(): SimpleOutput = new SimpleOutput {

    override def writeString(str: String): Unit = primitiveSink.putString(str, Charset.forName("UTF-8"))

    override def writeBoolean(boolean: Boolean): Unit = primitiveSink.putBoolean(boolean)

    override def writeInt(int: Int): Unit = primitiveSink.putInt(int)

    override def writeLong(long: Long): Unit = primitiveSink.putLong(long)

    override def writeDouble(double: Double): Unit = primitiveSink.putDouble(double)

    override def writeBigInt(bigInt: BigInt): Unit = primitiveSink.putBytes(bigInt.toByteArray)

    override def writeBigDecimal(bigDecimal: BigDecimal): Unit = primitiveSink.putInt(
      bigDecimal.hashCode())

    override def writeBinary(binary: Array[Byte]): Unit = primitiveSink.putBytes(binary)

  }

  override def writeList(): ListOutput = new PrimitiveSinkListOutput(this)

  override def writeObject(): ObjectOutput = new PrimitiveSinkObjectOutput(this)
}

class PrimitiveSinkListOutput(primitiveSinkOutput: PrimitiveSinkOutput) extends ListOutput {

  override def writeElement(): Output = primitiveSinkOutput

  override def finish(): Unit = {}
}

class PrimitiveSinkObjectOutput(primitiveSinkOutput: PrimitiveSinkOutput) extends ObjectOutput {

  override def writeField(key: String): Output = primitiveSinkOutput

  override def finish(): Unit = {}
}