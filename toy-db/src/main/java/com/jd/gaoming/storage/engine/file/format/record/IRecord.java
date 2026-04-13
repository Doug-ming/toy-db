package com.jd.gaoming.storage.engine.file.format.record;

public interface IRecord{
    byte[] toBytes();

    byte[] toBytes(int length);

    void fromBytes(byte[] bytes);

    int length();
}
