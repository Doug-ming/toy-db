package com.jd.gaoming.storage.engine.table.datatypes;

class Int32DataType implements IDataType<Integer> {
    @Override
    public boolean lengthIsVariable() {
        return false;
    }

    @Override
    public Class<Integer> javaType() {
        return Integer.class;
    }

    @Override
    public boolean hasPredefinedLength() {
        return true;
    }

    @Override
    public int predefinedLength() {
        return 4;
    }

    @Override
    public String typeName() {
        return "int32";
    }
}
