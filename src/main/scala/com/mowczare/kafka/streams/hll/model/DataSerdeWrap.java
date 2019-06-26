package com.mowczare.kafka.streams.hll.model;

import com.google.common.collect.Lists;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfItemsSerDe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class DataSerdeWrap<T> extends ArrayOfItemsSerDe<T> {

    ScalaArrayOfitemsSerde<T> real;
    public DataSerdeWrap(ScalaArrayOfitemsSerde<T> real) {
        this.real = real;
    }

    @Override
    public byte[] serializeToByteArray(T[] items) {
        return real.serializeToByteArray(Arrays.asList(items));
//        return new byte[0];
    }

    @Override
    public T[] deserializeFromMemory(Memory mem, int numItems) {
        return (T[]) real.deserializeFromMemory(mem, numItems).toArray(); //TODO
    }
}
