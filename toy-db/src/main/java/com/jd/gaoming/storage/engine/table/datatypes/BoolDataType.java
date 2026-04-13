package com.jd.gaoming.storage.engine.table.datatypes;

class BoolDataType implements IDataType<Boolean> {
    @Override
    public boolean lengthIsVariable() {
        return false;
    }

    @Override
    public Class<Boolean> javaType() {
        return Boolean.class;
    }

    @Override
    public boolean hasPredefinedLength() {
        return true;
    }

    @Override
    public int predefinedLength() {
        return 1;
    }

    @Override
    public String typeName() {
        return "bool";
    }
}
