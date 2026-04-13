package com.jd.gaoming.storage.engine.file.format.record;

import com.jd.gaoming.storage.engine.table.datatypes.IDataType;

public interface IMetaRecord extends IRecord{
    int columnLength();

    void columnLength(int columnLength);

    IDataType<?> columnDataType();

    void columnDataType(IDataType<?> dataType);

    int columnNameLength();

    void columnNameLength(int columnNameLength);

    String columnName();

    void columnName(String columnName);
}
