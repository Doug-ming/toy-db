package com.jd.gaoming.storage.engine.file.format.record;

import com.jd.gaoming.storage.engine.annotations.Nullable;

public interface IBranchRecord extends IRecord{
    @Nullable
    Comparable<?> key();

    long childPageNo();

    void key(Comparable<?> key);

    void childPageNo(long childPageNo);
}
