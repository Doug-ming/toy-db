package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.common.Constants;
import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.table.IColumn;
import com.jd.gaoming.storage.engine.table.IRow;
import com.jd.gaoming.storage.engine.table.ITableSchema;
import com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

public class LeafPageTest {
    @Test
    public void shouldWriteAndReadLeafPage() throws IOException {
        FileManager fileManager = new FileManager(new File("D:/DataFile/leaf.db"), Constants.FILE_HEADER_SIZE,Constants.PAGE_SIZE);

        IColumn[] columns = new IColumn[]{
                IColumn.createColumn(0,"employee_id",null, DataTypeRegistry.INT_32),
                IColumn.createColumn(1,"tax_number",4,DataTypeRegistry.CHAR),
                IColumn.createColumn(2,"hire_date",null,DataTypeRegistry.DATE),
                IColumn.createColumn(3,"gender",null,DataTypeRegistry.BOOL),
                IColumn.createColumn(4,"first_name",255,DataTypeRegistry.VARCHAR),
                IColumn.createColumn(5,"last_name",255,DataTypeRegistry.VARCHAR)
        };
        ITableSchema tableSchema = ITableSchema.createTableSchema(0,columns);

        IRow[] rows = new IRow[3];

        rows[0] = IRow.createEmptyRow(tableSchema);
        rows[0].setValue(0,1);
        rows[0].setValue(1,"ab");
        rows[0].setValue(2,new Date());
        rows[0].setValue(3,true);
        rows[0].setValue(4,"gao");
        rows[0].setValue(5,"ming");

        rows[1] = IRow.createEmptyRow(tableSchema);
        rows[1].setValue(0,2);
        rows[1].setValue(1,"cd");
        rows[1].setValue(2,new Date());
        rows[1].setValue(3,true);
        rows[1].setValue(4,"tan");
        rows[1].setValue(5,"ning");

        rows[2] = IRow.createEmptyRow(tableSchema);
        rows[2].setValue(0,3);
        rows[2].setValue(1,"ef");
        rows[2].setValue(2,new Date());
        rows[2].setValue(3,true);
        rows[2].setValue(4,"liu");
        rows[2].setValue(5,"meidi");

        ILeafPage leafPage = Pager.createMockLeafPage(fileManager,tableSchema,rows);
        leafPage.rewrite();
        fileManager.flushData(leafPage.internalBytes(),0L);

        ByteBuffer buf = fileManager.readData(0,Constants.PAGE_SIZE);
        ILeafPage anotherLeafPage = new LeafPage(fileManager,tableSchema);
        anotherLeafPage.wrap(buf,false);
        anotherLeafPage.refresh();

        Assert.assertTrue(!leafPage.equals(anotherLeafPage));
    }
}
