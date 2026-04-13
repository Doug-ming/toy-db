package com.jd.gaoming.storage.engine.serde;

import com.jd.gaoming.storage.engine.serde.interfaces.ToydbSerdes;
import com.jd.gaoming.storage.engine.table.IColumn;
import java.nio.ByteBuffer;

class BoolTypeSerdes implements ToydbSerdes<Boolean> {
    @Override
    public byte[] serialize(Boolean value, IColumn column) {
        return new byte[]{
                (byte) (value ? 1 : 0)
        };
    }

    @Override
    public void serialize(Boolean value, IColumn column, ByteBuffer buf) {
        buf.put((byte) (value ? 1 : 0));
    }

    @Override
    public Boolean deserialize(byte[] bytes, IColumn column) {
        return bytes[0] == 1;
    }
}
