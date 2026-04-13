package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.file.format.record.ILeafRecord;
import java.util.List;

public interface ILeafPage extends ISlottedPage{
    List<ILeafRecord> getLeafRecords();

    List<ILeafRecord> parseLeafRecords();

    void setLeafRecords(List<ILeafRecord> leafRecords);

    ILeafRecord createLeafRecord();
}
