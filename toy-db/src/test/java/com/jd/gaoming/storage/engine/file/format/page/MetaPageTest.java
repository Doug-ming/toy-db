package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.common.Constants;
import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.table.IColumn;
import com.jd.gaoming.storage.engine.table.ITableSchema;
import com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MetaPageTest {
    @Test
    public void shouldWriteAndReadMetaPage() throws IOException {
        File dataFile = new File("D:/DataFile/meta.db");

        FileManager fileManager = new FileManager(dataFile, Constants.FILE_HEADER_SIZE,Constants.PAGE_SIZE);
        final long rootPageNo = 1L;

        IColumn[] columns = new IColumn[]{
                IColumn.createColumn(0,"employee_id",null, DataTypeRegistry.INT_32),
                IColumn.createColumn(1,"tax_number",4,DataTypeRegistry.CHAR),
                IColumn.createColumn(2,"hire_date",null,DataTypeRegistry.DATE),
                IColumn.createColumn(3,"gender",null,DataTypeRegistry.BOOL),
                IColumn.createColumn(4,"first_name",255,DataTypeRegistry.VARCHAR),
                IColumn.createColumn(5,"last_name",255,DataTypeRegistry.VARCHAR)
        };

        ITableSchema tableSchema = ITableSchema.createTableSchema(0,columns);

        IMetaPage metaPage = Pager.createMetaPage(fileManager,rootPageNo,tableSchema);
        metaPage.rewrite();

        fileManager.flushData(metaPage.internalBytes(),0L);

        ByteBuffer buf = fileManager.readData(0L,Constants.PAGE_SIZE);

        IMetaPage anotherMetaPage = new MetaPage(fileManager);
        anotherMetaPage.wrap(buf,false);
        anotherMetaPage.refresh();

        Assert.assertTrue(metaPage.equals(anotherMetaPage));
    }
}
