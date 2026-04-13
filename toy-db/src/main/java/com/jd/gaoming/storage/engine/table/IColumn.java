package com.jd.gaoming.storage.engine.table;

import com.jd.gaoming.storage.engine.annotations.Immutable;
import com.jd.gaoming.storage.engine.table.datatypes.IDataType;

/**
 * 表示列定义的接口
 * */
@Immutable
public interface IColumn {
    String name();

    int position();

    int length();

    IDataType<?> type();

    static IColumn createColumn(int pos,String name,Integer length,IDataType<?> type){
        return new Column(pos,name,length,type);
    }
}
