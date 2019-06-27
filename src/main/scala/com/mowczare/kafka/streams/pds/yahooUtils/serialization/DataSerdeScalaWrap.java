package com.mowczare.kafka.streams.pds.yahooUtils.serialization;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfItemsSerDe;

import java.util.Arrays;

public class DataSerdeScalaWrap<T> extends ArrayOfItemsSerDe<T> {

    private ScalaArrayOfItemsSerde<T> real;

    public DataSerdeScalaWrap(ScalaArrayOfItemsSerde<T> real) {
        this.real = real;
    }

    @Override
    public byte[] serializeToByteArray(T[] items) {
        return real.serializeToByteArray(Arrays.asList(items));
    }

    @Override
    public T[] deserializeFromMemory(Memory mem, int numItems) {
        return (T[]) real.deserializeFromMemory(mem, numItems).toArray(); //TODO
    }
}
