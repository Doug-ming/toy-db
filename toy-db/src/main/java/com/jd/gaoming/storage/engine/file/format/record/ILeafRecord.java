package com.jd.gaoming.storage.engine.file.format.record;

import com.jd.gaoming.storage.engine.table.IRow;

public interface ILeafRecord extends IRecord{
    IRow toRow();

    void fromRow(IRow row);

    Comparable<?> getKey();
}
