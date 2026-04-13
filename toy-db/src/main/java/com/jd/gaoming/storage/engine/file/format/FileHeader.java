package com.jd.gaoming.storage.engine.file.format;

import com.jd.gaoming.storage.engine.common.Constants;
import java.nio.ByteBuffer;

public final class FileHeader {
    private final int magic;

    private final int version;

    private final int pageSize;

    public FileHeader(int magic,int version,int pageSize){
        this.magic = magic;
        this.version = version;
        this.pageSize = pageSize;
    }

    public int getMagic() {
        return magic;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getVersion() {
        return version;
    }

    public byte[] toBytes(){
        ByteBuffer buf = ByteBuffer.allocate(Constants.FILE_HEADER_SIZE);

        buf.putInt(magic);
        buf.putInt(version);
        buf.putInt(pageSize);

        return buf.array();
    }
}
