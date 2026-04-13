package com.jd.gaoming.storage.engine.table;

import java.util.*;

class TableSchema implements ITableSchema {
    private final IColumn pkColumn;

    private final IColumn[] columns;

    private final int posOfPk;

    private final List<String> columnNames;

    private final Map<String,IColumn> nameToColumns;

    TableSchema(int posOfPk,IColumn ... cols){
        this.posOfPk = posOfPk;
        columns = cols;
        pkColumn = columns[posOfPk];

        columnNames = new ArrayList<>();
        nameToColumns = new HashMap<>();
        for(IColumn col : cols){
            columnNames.add(col.name());
            nameToColumns.put(col.name(),col);
        }
    }

    @Override
    public String pkColumnName() {
        return pkColumn.name();
    }

    @Override
    public IColumn pkColumn() {
        return pkColumn;
    }

    @Override
    public List<String> columnNames() {
        return columnNames;
    }

    @Override
    public List<IColumn> columns() {
        return List.of(columns);
    }

    @Override
    public IColumn columnWithName(String columnName) {
        return nameToColumns.get(columnName);
    }

    @Override
    public boolean isPkColumn(String columnName) {
        return isPkColumn(columnWithName(columnName));
    }

    @Override
    public boolean isPkColumn(IColumn column) {
        return column.position() == posOfPk;
    }

    @Override
    public int count() {
        return columns.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableSchema that = (TableSchema) o;

        if(posOfPk != that.posOfPk)
            return false;

        final int n = count();
        for(int i = 0;i < n;i++){
            IColumn thisColumn = columns[i];
            IColumn thatColumn = that.columns[i];

            if(!thisColumn.equals(thatColumn))
                return false;
        }

        return true;
    }
}
