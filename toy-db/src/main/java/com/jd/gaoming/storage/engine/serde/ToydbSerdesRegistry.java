package com.jd.gaoming.storage.engine.serde;

import com.jd.gaoming.storage.engine.serde.interfaces.ToydbSerdes;
import com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry;
import com.jd.gaoming.storage.engine.table.datatypes.IDataType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ToydbSerdesRegistry {
    private static final Map<IDataType<?>, ToydbSerdes<?>> registry = new ConcurrentHashMap<>();

    static{
        registry.put(DataTypeRegistry.BOOL,new BoolTypeSerdes());
        registry.put(DataTypeRegistry.INT_32,new Int32TypeSerdes());
        registry.put(DataTypeRegistry.INT_64,new Int64TypeSerdes());
        registry.put(DataTypeRegistry.CHAR,new CharTypeSerdes());
        registry.put(DataTypeRegistry.VARCHAR,new VarcharTypeSerdes());
        registry.put(DataTypeRegistry.DATE,new DateTypeSerdes());
        registry.put(DataTypeRegistry.DATETIME,new DatetimeTypeSerdes());
    }

    public static ToydbSerdes<?> getInstance(IDataType<?> dataType){
        return registry.get(dataType);
    }
}
