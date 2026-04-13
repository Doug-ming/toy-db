package com.jd.gaoming.storage.engine.table;

import com.jd.gaoming.storage.engine.file.format.record.IRecord;
import com.jd.gaoming.storage.engine.tree.BTree;

public final class Table implements ITable{
    private final String tableName;

    private final ITableSchema tableSchema;

    private final BTree bTree;

    public Table(String tableName, ITableSchema tableSchema,BTree bTree) {
        this.bTree = bTree;
        this.tableName = tableName;
        this.tableSchema = tableSchema;
    }


    @Override
    public String tableName() {
        return tableName;
    }

    @Override
    public ITableSchema tableSchema() {
        return tableSchema;
    }

    @Override
    public void insert(IRow row) {
        bTree.insert(row);
    }

    @Override
    public IRow search(Comparable<?> key) {
        IRecord record = bTree.search(key);
        if(record == null)
            throw new IllegalStateException("record " + key + " not found");
        return bTree.search(key).toRow();
    }
}
