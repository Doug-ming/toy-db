package com.jd.gaoming.storage.engine;

import com.jd.gaoming.storage.engine.common.Constants;
import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.file.format.FileHeader;
import com.jd.gaoming.storage.engine.file.format.page.Pager;
import com.jd.gaoming.storage.engine.file.format.page.IMetaPage;
import com.jd.gaoming.storage.engine.file.format.record.IMetaRecord;
import com.jd.gaoming.storage.engine.table.IColumn;
import com.jd.gaoming.storage.engine.table.ITable;
import com.jd.gaoming.storage.engine.table.ITableSchema;
import com.jd.gaoming.storage.engine.table.Table;
import com.jd.gaoming.storage.engine.table.datatypes.IDataType;
import com.jd.gaoming.storage.engine.tree.BTree;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ToyDB {
    private final String dataDirPath;

    private final Map<String,ITable> tableMap = new ConcurrentHashMap<>();

    public ToyDB(String dataDirPath) {
        if(!dataDirPath.endsWith(File.separator))
            dataDirPath = dataDirPath + File.separator;

        this.dataDirPath = dataDirPath;
    }

    public void open(){
        File dataDir = new File(dataDirPath);

        if(!dataDir.exists())
            dataDir.mkdir();
        else if(!dataDir.isDirectory())
            throw new IllegalStateException(String.format("Data directory path %s is not valid.",dataDirPath));

        File[] dataFiles = dataDir.listFiles();

        Arrays.stream(dataFiles).forEach((dataFile) -> {
            initMetaPage(dataFile);
        });
    }

    private void initMetaPage(File dataFile) {
        String tableName = tableNameFromDataFile(dataFile);

        FileManager fileManager = new FileManager(dataFile,Constants.FILE_HEADER_SIZE,Constants.PAGE_SIZE);
        IMetaPage metaPage = Pager.restoreMetaPage(fileManager,tableName);

        MetaPageCache.addMetaPage(tableName,metaPage);
    }

    private String tableNameFromDataFile(File dataFile) {
        String fileName = dataFile.getName();
        return fileName.substring(0,fileName.length() - 3);
    }

    public ITable from(String tableName){
        ITable table = tableMap.get(tableName);

        if(table == null){
            IMetaPage metaPage = MetaPageCache.getMetaPage(tableName);
            ITableSchema tableSchema = parseTableSchema(metaPage.getMetaRecords());

            FileManager fileManager = new FileManager(new File(dataDirPath + tableName + ".db"),Constants.FILE_HEADER_SIZE,Constants.PAGE_SIZE);

            if(metaPage == null)
                throw new IllegalArgumentException("Table " + tableName + " does not exist.");

            long rootPageNo = metaPage.multipurposePageNo();
            BTree tree = (rootPageNo == 0L) ? BTree.createTree(fileManager,tableName,tableSchema) : BTree.restoreTree(fileManager,tableName);

            table = new Table(tableName,tableSchema,tree);
            tableMap.put(tableName,table);
        }

        return table;
    }

    public synchronized ITable createTable(String tableName,ITableSchema tableSchema){
        if(tableMap.containsKey(tableName)){
            throw new IllegalArgumentException("Table " + tableName + " already exists.");
        }

        File dataFile = new File(dataDirPath + tableName + ".db");
        FileManager fileManager = new FileManager(dataFile,Constants.FILE_HEADER_SIZE,Constants.PAGE_SIZE);

        FileHeader fileHeader = new FileHeader(Constants.MAGIC,Constants.VERSION,Constants.PAGE_SIZE);
        fileManager.flushData(ByteBuffer.wrap(fileHeader.toBytes()),0L);

        IMetaPage metaPage = Pager.createMetaPage(fileManager,0L,tableSchema);
        metaPage.rewrite();
        metaPage.flush();

        MetaPageCache.addMetaPage(tableName,metaPage);
        metaPage.flush();

        BTree tree = BTree.createTree(fileManager,tableName,tableSchema);

        return new Table(tableName,tableSchema,tree);
    }

    private ITableSchema parseTableSchema(List<IMetaRecord> metaRecords) {
        IColumn[] columns = new IColumn[metaRecords.size()];
        for(int i = 0;i < metaRecords.size();i++){
            IMetaRecord metaRecord = metaRecords.get(i);

            final int pos = i;
            final String columnName = metaRecord.columnName();
            final int columnLength = metaRecord.columnLength();
            IDataType<?> columnType = metaRecord.columnDataType();

            columns[i] = IColumn.createColumn(pos,columnName,columnLength,columnType);
        }

        return ITableSchema.createTableSchema(0,columns);
    }
}
