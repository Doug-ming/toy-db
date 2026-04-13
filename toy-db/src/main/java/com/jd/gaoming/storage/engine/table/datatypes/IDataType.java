package com.jd.gaoming.storage.engine.table.datatypes;

import com.jd.gaoming.storage.engine.annotations.Immutable;

/**
 * 表示存储引擎内置数据类型的接口
 * */
@Immutable
public interface IDataType<JavaType> {
    boolean lengthIsVariable();

    Class<JavaType> javaType();

    boolean hasPredefinedLength();

    int predefinedLength();

    String typeName();
}
