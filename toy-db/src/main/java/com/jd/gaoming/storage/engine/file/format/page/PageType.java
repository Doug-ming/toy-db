package com.jd.gaoming.storage.engine.file.format.page;

public enum PageType {
    META(0x01),BRANCH(0x02),LEAF(0x04);

    public final int mask;

    PageType(int mask){
        this.mask = mask;
    }
}
