package com.jd.gaoming.storage.engine.table.datatypes;

class Int64DataType implements IDataType<Long> {

    @Override
    public boolean lengthIsVariable() {
        return false;
    }

    @Override
    public Class<Long> javaType() {
        return Long.class;
    }

    @Override
    public boolean hasPredefinedLength() {
        return true;
    }

    @Override
    public int predefinedLength() {
        return 8;
    }

    @Override
    public String typeName() {
        return "int64";
    }
}
