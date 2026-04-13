package com.jd.gaoming.storage.engine.table.datatypes;

import java.util.Date;

class DatetimeDataType implements IDataType<Date> {

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
        return 7;
    }

    @Override
    public String typeName() {
        return "datetime";
    }
}
