package com.jd.gaoming.storage.engine.table.datatypes;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataTypeRegistry {
    public static final BoolDataType BOOL = new BoolDataType();
    public static final Int32DataType INT_32 = new Int32DataType();
    public static final Int64DataType INT_64 = new Int64DataType();
    public static final CharDataType CHAR = new CharDataType();
    public static final VarcharDataType VARCHAR = new VarcharDataType();
    public static final DateDataType DATE = new DateDataType();
    public static final DatetimeDataType DATETIME = new DatetimeDataType();

    private final static Map<IDataType<?>,Integer> TYPE_TO_CODE = new ConcurrentHashMap<>();

    private final static Map<Integer,IDataType<?>> CODE_TO_TYPE = new ConcurrentHashMap<>();

    static{
        TYPE_TO_CODE.put(BOOL,0);
        TYPE_TO_CODE.put(INT_32,1);
        TYPE_TO_CODE.put(INT_64,2);
        TYPE_TO_CODE.put(CHAR,3);
        TYPE_TO_CODE.put(VARCHAR,4);
        TYPE_TO_CODE.put(DATE,5);
        TYPE_TO_CODE.put(DATETIME,6);

        CODE_TO_TYPE.put(0,BOOL);
        CODE_TO_TYPE.put(1,INT_32);
        CODE_TO_TYPE.put(2,INT_64);
        CODE_TO_TYPE.put(3,CHAR);
        CODE_TO_TYPE.put(4,VARCHAR);
        CODE_TO_TYPE.put(5,DATE);
        CODE_TO_TYPE.put(6,DATETIME);
    }

    public static int typeCode(IDataType<?> type){
        return TYPE_TO_CODE.get(type);
    }

    public static IDataType<?> forTypeCode(int typeCode){
        IDataType<?> dataType = CODE_TO_TYPE.get(typeCode);

        if(dataType == null)
            throw new IllegalArgumentException(String.format("type code %d does not exist.",typeCode));

        return dataType;
    }
}
