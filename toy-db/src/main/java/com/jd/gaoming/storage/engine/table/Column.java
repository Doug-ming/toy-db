package com.jd.gaoming.storage.engine.table;

import com.jd.gaoming.storage.engine.annotations.Nullable;
import com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry;
import com.jd.gaoming.storage.engine.table.datatypes.IDataType;

class Column implements IColumn {
    private final int pos;

    private final String name;

    @Nullable
    private final Integer length;

    private final IDataType<?> type;

    Column(int pos,String name,Integer length,int typeCode){
        this(pos,name,length, DataTypeRegistry.forTypeCode(typeCode));
    }

    Column(int pos,String name,Integer length,IDataType<?> type){
        this.pos = pos;
        this.name = name;
        this.length = length;
        this.type = type;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int position() {
        return pos;
    }

    @Override
    public int length() {
        if(length != null)
            return length;
        else if(type.hasPredefinedLength())
            return type.predefinedLength();

        throw new IllegalStateException(String.format("Cannot calculate length of column %s",name));
    }

    @Override
    public IDataType<?> type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Column that = (Column) o;

        if(length == null && that.length != null)
            return false;

        return pos == that.pos && type == that.type && (length == that.length || length.equals(that.length)) && name.equals(that.name);
    }
}
