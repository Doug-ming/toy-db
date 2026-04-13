package com.jd.gaoming.storage.engine.serde;

import com.jd.gaoming.storage.engine.serde.interfaces.ToydbSerdes;
import com.jd.gaoming.storage.engine.table.IColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class VarcharTypeSerdes implements ToydbSerdes<String> {
    @Override
    public byte[] serialize(String value, IColumn column) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void serialize(String value, IColumn column, ByteBuffer buf) {
        buf.put(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String deserialize(byte[] bytes, IColumn column) {
        return new String(bytes,StandardCharsets.UTF_8);
    }
}
