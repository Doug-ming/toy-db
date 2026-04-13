package com.jd.gaoming.storage.engine.table.datatypes;

class CharDataType implements IDataType<String> {
    @Override
    public boolean lengthIsVariable() {
        return false;
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
        throw new UnsupportedOperationException("char type has no predefined length.");
    }

    @Override
    public String typeName() {
        return "char";
    }
}
