package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.file.format.record.IBranchRecord;
import java.util.List;

public interface IBranchPage extends ISlottedPage{
    List<IBranchRecord> getBranchRecords();

    List<IBranchRecord> parseBranchRecords();

    void setBranchRecords(List<IBranchRecord> branchRecords);

    IBranchRecord createBranchRecord();

    IBranchRecord createBranchRecord(long childPageNo,Comparable<?> key);
}
