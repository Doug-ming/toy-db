package com.jd.gaoming.storage.engine.table.datatypes;

import java.util.Date;

class DateDataType implements IDataType<Date> {
    @Override
    public boolean lengthIsVariable() {
        return false;
    }

    @Override
    public Class<Date> javaType() {
        return Date.class;
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
        return "date";
    }
}
