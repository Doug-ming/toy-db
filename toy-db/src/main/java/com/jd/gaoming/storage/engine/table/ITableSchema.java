package com.jd.gaoming.storage.engine.table;

import com.jd.gaoming.storage.engine.annotations.Immutable;
import com.jd.gaoming.storage.engine.file.format.page.IMetaPage;
import com.jd.gaoming.storage.engine.file.format.record.IMetaRecord;
import com.jd.gaoming.storage.engine.table.datatypes.IDataType;
import java.util.List;

/**
 * 表示表定义的接口
 * */
@Immutable
public interface ITableSchema {
    String pkColumnName();

    IColumn pkColumn();

    List<String> columnNames();

    List<IColumn> columns();

    IColumn columnWithName(String name);

    boolean isPkColumn(String columnName);

    boolean isPkColumn(IColumn column);

    int count();

    static ITableSchema createTableSchema(int posOfPk,IColumn ... columns){
        return new TableSchema(posOfPk,columns);
    }

    static ITableSchema restoreTableSchema(IMetaPage metaPage,int posOfPk){
        List<IMetaRecord> metaRecords = metaPage.getMetaRecords();

        IColumn[] columns = getColumnsFromMetaRecords(metaRecords);
        return ITableSchema.createTableSchema(posOfPk,columns);
    }

    private static IColumn[] getColumnsFromMetaRecords(List<IMetaRecord> metaRecords) {
        IColumn[] columns = new IColumn[metaRecords.size()];

        for(int i = 0;i < columns.length;i++){
            IMetaRecord metaRecord = metaRecords.get(i);

            final int pos = i;
            final IDataType dataType = metaRecord.columnDataType();
            final int columnLength = metaRecord.columnLength();
            final String columnName = metaRecord.columnName();

            columns[i] = IColumn.createColumn(pos,columnName,columnLength,dataType);
        }

        return columns;
    }
}
