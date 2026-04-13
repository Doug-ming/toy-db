package com.jd.gaoming.storage.engine.table.datatypes;

class VarcharDataType implements IDataType<String> {
    @Override
    public boolean lengthIsVariable() {
        return true;
    }

    @Override
    public Class<String> javaType() {
        return String.class;
    }

    @Override
    public boolean hasPredefinedLength() {
        return false;
    }

    @Override
    public int predefinedLength() {
        throw new UnsupportedOperationException("varchar type has no defined length.");
    }

    @Override
    public String typeName() {
        return "varchar";
    }
}
