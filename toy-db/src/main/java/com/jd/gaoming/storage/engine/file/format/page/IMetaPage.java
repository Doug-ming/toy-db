package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.file.format.record.IMetaRecord;
import com.jd.gaoming.storage.engine.table.datatypes.IDataType;
import java.util.List;

public interface IMetaPage extends ISlottedPage {
    List<IMetaRecord> getMetaRecords();

    List<IMetaRecord> parseMetaRecords();

    void setMetaRecords(List<IMetaRecord> metaRecords);

    IMetaRecord createMetaRecord();

    IMetaRecord createMetaRecord(int columnLength, IDataType<?> dataType,int columnNameLength,String columnName);
}
