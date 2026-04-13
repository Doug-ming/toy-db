package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.exceptions.DataAccessException;
import java.nio.ByteBuffer;

public interface IPage {
    long pageNo();

    void pageNo(long pageNo);

    PageType pageType();

    int flags();

    void flags(int flags);

    int count();

    void count(int c);

    int overflow();

    void overflow(int overflow);

    long checksum();

    void checksum(long checksum);

    long multipurposePageNo();

    void multipurposePageNo(long multipurposePageNo);

    long nextSiblingPageNo();

    void nextSiblingPageNo(long nextSiblingPageNo);

    void wrap(ByteBuffer buf,boolean eagerlyParse);

    void wrap(ByteBuffer buf);

    /**
     * Refresh running state of page using backend byte buffer
     * */
    void refresh();

    /**
     * Rewrite full backend byte buffer using running state of page
     * */
    void rewrite();

    /**
     * return size of free space of backend byte buffer
     * */
    int freeSpaceSize();

    /**
     * check whether the current page is dirty
     * */
    boolean isDirty();

    void setDirty(boolean dirty);

    ByteBuffer internalBytes();

    /**
     * 将内存中的Page数据刷盘
     * */
    void flush() throws DataAccessException;
}
