package com.jd.gaoming.storage.engine.file.format;

import com.jd.gaoming.storage.engine.common.Constants;
import com.jd.gaoming.storage.engine.file.format.record.ISlotDirItem;
import java.nio.ByteBuffer;

public abstract class AbstractSlotDirItem implements ISlotDirItem {
    protected final int index;

    protected int pos;

    protected int size;

    public AbstractSlotDirItem(int index){
        this.index = index;
    }

    public AbstractSlotDirItem(int index,int pos,int size){
        this.index = index;
        this.pos = pos;
        this.size = size;
    }

    @Override
    public int pos() {
        return pos;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void pos(int pos){
        this.pos = pos;
    }

    @Override
    public void size(int size){
        this.size = size;
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putInt(pos);
        buf.putInt(size);
        return buf.array();
    }

    @Override
    public void fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        pos = buf.getInt();
        size = buf.getInt();
    }

    @Override
    public int length() {
        return Constants.SLOT_DIR_ITEM_SIZE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractSlotDirItem that = (AbstractSlotDirItem) o;
        return index == that.index && pos == that.pos && size == that.size;
    }
}
