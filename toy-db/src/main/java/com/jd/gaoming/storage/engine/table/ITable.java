package com.jd.gaoming.storage.engine.table;

public interface ITable {
    String tableName();

    ITableSchema tableSchema();

    void insert(IRow row);

    IRow search(Comparable<?> key);
}
