package com.jd.gaoming.storage.engine.file.format.record;

public interface ISlotDirItem {
    int pos();

    int size();

    void pos(int pos);

    void size(int size);

    byte[] toBytes();

    void fromBytes(byte[] bytes);

    IRecord record();

    int length();
}
