package com.jd.gaoming.storage.engine.common;

public interface Constants {
    byte[] MAGIC_BYTES = {0x78,0x7a,0x70,0x70};

    int MAGIC = 2021290096;

    int FILE_HEADER_SIZE = 12;

    int PAGE_SIZE = 4096;

    int PAGE_HEADER_SIZE = 26;

    int VERSION = 1;

    int SLOT_DIR_ITEM_SIZE = 8;
}
