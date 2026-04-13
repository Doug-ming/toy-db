package com.jd.gaoming.storage.engine.serde.interfaces;

import com.jd.gaoming.storage.engine.table.IColumn;
import java.nio.ByteBuffer;

public interface ToydbSerdes<JavaType> {
    byte[] serialize(JavaType value, IColumn column);

    void serialize(JavaType value, IColumn column, ByteBuffer buf);

    JavaType deserialize(byte[] bytes,IColumn column);

//    JavaType deserialize(ByteBuffer buf,IColumn column);
}
