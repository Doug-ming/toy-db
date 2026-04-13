package com.jd.gaoming.storage.engine;

import com.jd.gaoming.storage.engine.table.IColumn;
import com.jd.gaoming.storage.engine.table.IRow;
import com.jd.gaoming.storage.engine.table.ITable;
import com.jd.gaoming.storage.engine.table.ITableSchema;
import com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry;
import org.junit.Assert;
import org.junit.Test;
import java.util.*;

public class DBEngineTest {
    @Test
    public void shouldInsertRows(){
        ToyDB db = new ToyDB("D:/DataFile");

        db.open();

        final ITableSchema tableSchema = ITableSchema.createTableSchema(0,
                IColumn.createColumn(0,"employee_id",null, DataTypeRegistry.INT_32),
                IColumn.createColumn(1,"tax_number",4,DataTypeRegistry.CHAR),
                IColumn.createColumn(2,"hire_date",null,DataTypeRegistry.DATE),
                IColumn.createColumn(3,"is_male",null,DataTypeRegistry.BOOL),
                IColumn.createColumn(4,"first_name",255,DataTypeRegistry.VARCHAR),
                IColumn.createColumn(5,"last_name",255,DataTypeRegistry.VARCHAR));

        ITable table = db.createTable("employee",tableSchema);
        final int n = 100000;

        List<IRow> rows = generateRandomRows(n,tableSchema);
        Collections.shuffle(rows);

        rows.forEach((row) -> {
            table.insert(row);
        });

        Collections.sort(rows,(r1,r2) -> (Integer) r1.getValue(0) - (Integer) r2.getValue(0));

        List<Integer> keys = generateINT32Keys(n);
        Collections.shuffle(keys);

        keys.forEach((key) -> {
            IRow row_1 = rows.get(key - 1);
            IRow row_2 = table.search(key);

            Assert.assertTrue(rowEqual(row_1,row_2));
        });
    }

    @Test
    public void shouldLoadRowsFromDisk(){
        ToyDB db = new ToyDB("D:/DataFile");

        db.open();

        ITable table = db.from("employee");
        final int n = 100000;

        List<IRow> rows = generateRandomRows(n,table.tableSchema());

        List<Integer> keys = generateINT32Keys(n);
        Collections.shuffle(keys);

        keys.forEach((key) -> {
            IRow row_1 = rows.get(key - 1);
            IRow row_2 = table.search(key);

            Assert.assertTrue(rowEqual(row_1,row_2));
        });
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

    private List<Integer> generateINT32Keys(int n){
        List<Integer> keys = new ArrayList<>(n);

        for(int i = 1;i <= n;i++){
            keys.add(i);
        }

        return keys;
    }

    private List<IRow> generateRandomRows(int n, ITableSchema tableSchema) {
        List<IRow> rows = new ArrayList<>(n);
        for(int i = 0;i < n;i++){
            IRow row = IRow.createEmptyRow(tableSchema);
            row.setValue(0,i + 1);
            row.setValue(1,"ab");
            row.setValue(2,new Date());
            row.setValue(3,i % 4 == 0);
            row.setValue(4,"firstname" + i);
            row.setValue(5,"lastname" + i);
            rows.add(row);
        }
        return rows;
    }
}
