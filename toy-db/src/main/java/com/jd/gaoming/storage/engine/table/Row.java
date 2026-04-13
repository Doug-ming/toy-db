package com.jd.gaoming.storage.engine.table;

import java.util.Objects;

class Row implements IRow {
    final ITableSchema tableSchema;
    final Object[] values;

    Row(ITableSchema tableSchema){
        this.tableSchema = tableSchema;
        values = new Object[tableSchema.count()];
    }

    @Override
    public Object getValue(int index) {
        return values[index];
    }

    @Override
    public Object getValue(String columnName) {
        IColumn column = tableSchema.columnWithName(columnName);
        return values[column.position()];
    }

    @Override
    public void setValue(String columnName, Object value) {
        IColumn column = tableSchema.columnWithName(columnName);
        setValue(column.position(),value);
    }

    @Override
    public void setValue(int i, Object value) {
        values[i] = value;
    }

    @Override
    public int fieldCount() {
        return values.length;
    }

    @Override
    public Comparable<?> getKey() {
        return (Comparable<?>) values[tableSchema.pkColumn().position()];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row that = (Row) o;
        return tableSchema == that.tableSchema && Objects.deepEquals(values, that.values);
    }
}
