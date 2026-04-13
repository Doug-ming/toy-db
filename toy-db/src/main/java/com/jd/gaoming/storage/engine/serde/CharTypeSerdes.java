package com.jd.gaoming.storage.engine.serde;

import com.jd.gaoming.storage.engine.serde.interfaces.ToydbSerdes;
import com.jd.gaoming.storage.engine.table.IColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class CharTypeSerdes implements ToydbSerdes<String> {

    @Override
    public byte[] serialize(String value, IColumn column) {
        final int width = column.length();

        if(value.length() < width){
            int n = width - value.length();
            for(int i = 0;i < n;i++)
                value = value + " ";
        }

        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void serialize(String value, IColumn column, ByteBuffer buf) {
        buf.put(serialize(value,column));
    }

    @Override
    public String deserialize(byte[] bytes, IColumn column) {
        return new String(bytes,StandardCharsets.UTF_8);
    }
}
