package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.common.Constants;
import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.table.IColumn;
import com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BranchPageTest {
    @Test
    public void shouldWriteAndReadBranchPageCorrectly() throws IOException {
        File dataFile = new File("D:/DataFile/branch.db");

        FileManager fileManager = new FileManager(dataFile, Constants.FILE_HEADER_SIZE,Constants.PAGE_SIZE);
        IColumn keyColumn = IColumn.createColumn(0,"employee_id",null, DataTypeRegistry.INT_32);

        Pager.IndexItem[] indexItems = new Pager.IndexItem[4];
        indexItems[0] = new Pager.IndexItem(5,2);
        indexItems[1] = new Pager.IndexItem(10,3);
        indexItems[2] = new Pager.IndexItem(15,4);
        indexItems[3] = new Pager.IndexItem(null,5);

        IBranchPage branchPage = Pager.createMockBranchPage(fileManager,keyColumn,indexItems);
        branchPage.rewrite();

        fileManager.flushData(branchPage.internalBytes(),0L);

        ByteBuffer buf = fileManager.readData(0L,Constants.PAGE_SIZE);
        IBranchPage anotherBranchPage = new BranchPage(fileManager,keyColumn);
        anotherBranchPage.wrap(buf,false);
        anotherBranchPage.refresh();

        Assert.assertTrue(branchPage.equals(anotherBranchPage));
    }
}
