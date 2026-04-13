package com.jd.gaoming.storage.engine.table;

public interface IRow {
    Object getValue(int index);

    Object getValue(String columnName);

    void setValue(String columnName,Object value);

    void setValue(int i,Object value);

    int fieldCount();

    Comparable<?> getKey();

    static IRow createEmptyRow(ITableSchema tableSchema){
        return new Row(tableSchema);
    }
}
