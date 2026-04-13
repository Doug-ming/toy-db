package com.jd.gaoming.storage.engine.tree;

import com.jd.gaoming.storage.engine.common.Constants;
import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.file.format.FileHeader;
import com.jd.gaoming.storage.engine.file.format.page.Pager;
import com.jd.gaoming.storage.engine.table.IColumn;
import com.jd.gaoming.storage.engine.table.IRow;
import com.jd.gaoming.storage.engine.table.ITableSchema;
import com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import com.jd.gaoming.storage.engine.file.format.page.IMetaPage;
import org.junit.Assert;
import org.junit.Test;

public class BTreeTest {
    @Test
    public void shouldInsertFewRows(){
        final ITableSchema tableSchema = ITableSchema.createTableSchema(0,
                IColumn.createColumn(0,"employee_id",null, DataTypeRegistry.INT_32),
                IColumn.createColumn(1,"tax_number",4,DataTypeRegistry.CHAR),
                IColumn.createColumn(2,"hire_date",null,DataTypeRegistry.DATE),
                IColumn.createColumn(3,"is_male",null,DataTypeRegistry.BOOL),
                IColumn.createColumn(4,"first_name",255,DataTypeRegistry.VARCHAR),
                IColumn.createColumn(5,"last_name",255,DataTypeRegistry.VARCHAR));

        final FileManager fileManager = new FileManager(
                new File("D:/DataFile/employee.db"),
                Constants.FILE_HEADER_SIZE,
                Constants.PAGE_SIZE
        );

        //write file header
        FileHeader fileHeader = new FileHeader(Constants.MAGIC,1,Constants.PAGE_SIZE);
        fileManager.flushData(ByteBuffer.wrap(fileHeader.toBytes()),0L);

        //write meta page
        IMetaPage metaPage = Pager.createMetaPage(fileManager,1L,tableSchema);
        metaPage.rewrite();
        metaPage.flush();

        //insert rows
        IRow[] rows = new IRow[5];

        rows[0] = IRow.createEmptyRow(tableSchema);
        rows[0].setValue(0,1);
        rows[0].setValue(1,"sdc");
        rows[0].setValue(2,new Date());
        rows[0].setValue(3,true);
        rows[0].setValue(4,"gao");
        rows[0].setValue(5,"ming");

        rows[1] = IRow.createEmptyRow(tableSchema);
        rows[1].setValue(0,2);
        rows[1].setValue(1,"tghc");
        rows[1].setValue(2,new Date());
        rows[1].setValue(3,false);
        rows[1].setValue(4,"liu");
        rows[1].setValue(5,"meidi");

        rows[2] = IRow.createEmptyRow(tableSchema);
        rows[2].setValue(0,3);
        rows[2].setValue(1,"ne");
        rows[2].setValue(2,new Date());
        rows[2].setValue(3,false);
        rows[2].setValue(4,"tan");
        rows[2].setValue(5,"ning");

        rows[3] = IRow.createEmptyRow(tableSchema);
        rows[3].setValue(0,4);
        rows[3].setValue(1,"bnj");
        rows[3].setValue(2,new Date());
        rows[3].setValue(3,true);
        rows[3].setValue(4,"xie");
        rows[3].setValue(5,"daheng");

        rows[4] = IRow.createEmptyRow(tableSchema);
        rows[4].setValue(0,5);
        rows[4].setValue(1,"try");
        rows[4].setValue(2,new Date());
        rows[4].setValue(3,true);
        rows[4].setValue(4,"zuo");
        rows[4].setValue(5,"chenyun");

        BTree tree = BTree.createTree(fileManager,"employee",tableSchema);

        tree.insert(rows[0]);
        tree.insert(rows[1]);
        tree.insert(rows[2]);
        tree.insert(rows[3]);
        tree.insert(rows[4]);

        IRow row_1 = tree.search(1).toRow();
        Assert.assertTrue(rowEqual(rows[0],row_1));

        IRow row_2 = tree.search(2).toRow();
        Assert.assertTrue(rowEqual(rows[1],row_2));

        IRow row_3 = tree.search(3).toRow();
        Assert.assertTrue(rowEqual(rows[2],row_3));

        IRow row_4 = tree.search(4).toRow();
        Assert.assertTrue(rowEqual(rows[3],row_4));

        IRow row_5 = tree.search(5).toRow();
        Assert.assertTrue(rowEqual(rows[4],row_5));

    }

    @Test
    public void shouldInsertManyRows(){
        final ITableSchema tableSchema = ITableSchema.createTableSchema(0,
                IColumn.createColumn(0,"employee_id",null, DataTypeRegistry.INT_32),
                IColumn.createColumn(1,"tax_number",4,DataTypeRegistry.CHAR),
                IColumn.createColumn(2,"hire_date",null,DataTypeRegistry.DATE),
                IColumn.createColumn(3,"is_male",null,DataTypeRegistry.BOOL),
                IColumn.createColumn(4,"first_name",255,DataTypeRegistry.VARCHAR),
                IColumn.createColumn(5,"last_name",255,DataTypeRegistry.VARCHAR));

        final FileManager fileManager = new FileManager(
                new File("D:/DataFile/employee.db"),
                Constants.FILE_HEADER_SIZE,
                Constants.PAGE_SIZE
        );

        //write file header
        FileHeader fileHeader = new FileHeader(Constants.MAGIC,1,Constants.PAGE_SIZE);
        fileManager.flushData(ByteBuffer.wrap(fileHeader.toBytes()),0L);

        //write meta page
        IMetaPage metaPage = Pager.createMetaPage(fileManager,1L,tableSchema);
        metaPage.rewrite();
        metaPage.flush();

        BTree tree = BTree.createTree(fileManager,"employee",tableSchema);

        final int n = 1000;
        List<IRow> rows = generateRandomRows(n,tableSchema);
        Collections.shuffle(rows);

        long begin = System.nanoTime();
        rows.forEach(tree :: insert);
        System.out.println(String.format("insert 1000 records consume %d ns",(System.nanoTime() - begin)));

        List<Integer> keys = new ArrayList<>(n);
        for(int i = 0;i < n;i++){
            keys.add(i + 1);
        }
        Collections.shuffle(keys);

        Collections.sort(rows,(r1,r2) -> (Integer) r1.getValue(0) - (Integer) r2.getValue(0));
        for(Integer key : keys){
            IRow row_1 = rows.get(key - 1);
            IRow row_2 = tree.search(key).toRow();

            Assert.assertTrue(rowEqual(row_1,row_2));
        }
    }

    @Test
    public void shouldLoadManyRows(){
        FileManager fileManager = new FileManager(new File("D:/DataFile/employee.db"),Constants.FILE_HEADER_SIZE,Constants.PAGE_SIZE);

        BTree tree = BTree.restoreTree(fileManager,"employee");

        final ITableSchema tableSchema = ITableSchema.createTableSchema(0,
                IColumn.createColumn(0,"employee_id",null, DataTypeRegistry.INT_32),
                IColumn.createColumn(1,"tax_number",4,DataTypeRegistry.CHAR),
                IColumn.createColumn(2,"hire_date",null,DataTypeRegistry.DATE),
                IColumn.createColumn(3,"is_male",null,DataTypeRegistry.BOOL),
                IColumn.createColumn(4,"first_name",255,DataTypeRegistry.VARCHAR),
                IColumn.createColumn(5,"last_name",255,DataTypeRegistry.VARCHAR));

        List<IRow> rows = generateRandomRows(1000,tableSchema);
        Collections.shuffle(rows);

        rows.forEach((row) -> {
            IRow row_1 = tree.search(row.getKey()).toRow();
            Assert.assertTrue(rowEqual(row,row_1));
        });
    }

    private List<IRow> generateRandomRows(int n,ITableSchema tableSchema) {
        List<IRow> rows = new ArrayList<>(n);
        for(int i = 0;i < n;i++){
            IRow row = IRow.createEmptyRow(tableSchema);
            row.setValue(0,i + 1);
            row.setValue(1,String.valueOf(i + 1));
            row.setValue(2,new Date());
            row.setValue(3,i % 4 == 0);
            row.setValue(4,"firstname" + i);
            row.setValue(5,"lastname" + i);
            rows.add(row);
        }
        return rows;
    }

    private boolean rowEqual(IRow row_1,IRow row_2){
        if(!row_1.getValue(0).equals(row_2.getValue(0)))
            return false;

        if(!String.valueOf(row_1.getValue(1)).trim().equals(String.valueOf(row_2.getValue(1)).trim()))
            return false;

        if(!row_1.getValue(3).equals(row_2.getValue(3)))
            return false;

        if(!row_1.getValue(4).equals(row_2.getValue(4)))
            return false;

        if(!row_1.getValue(5).equals(row_2.getValue(5)))
            return false;

        Date date_1 = (Date) row_1.getValue(2);
        Date date_2 = (Date) row_2.getValue(2);
        return dateEquals(date_1,date_2);
    }

    private boolean dateEquals(Date date1, Date date2) {
        Calendar cal_1 = Calendar.getInstance();
        cal_1.setTime(date1);

        int year_1 = cal_1.get(Calendar.YEAR);
        int month_1 = cal_1.get(Calendar.MONTH);
        int day_1 = cal_1.get(Calendar.DAY_OF_MONTH);

        Calendar cal_2 = Calendar.getInstance();
        cal_2.setTime(date2);

        int year_2 = cal_2.get(Calendar.YEAR);
        int month_2 = cal_2.get(Calendar.MONTH);
        int day_2 = cal_2.get(Calendar.DAY_OF_MONTH);

        return year_1 == year_2 && month_1 == month_2 && day_1 == day_2;
    }
}
