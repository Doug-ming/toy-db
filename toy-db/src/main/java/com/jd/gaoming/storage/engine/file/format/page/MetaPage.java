package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.file.format.AbstractSlotDirItem;
import com.jd.gaoming.storage.engine.file.format.record.IMetaRecord;
import com.jd.gaoming.storage.engine.file.format.record.IRecord;
import com.jd.gaoming.storage.engine.file.format.record.ISlotDirItem;
import com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry;
import com.jd.gaoming.storage.engine.table.datatypes.IDataType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class MetaPage extends AbstractSlottedPage implements IMetaPage {
    List<IMetaRecord> metaRecords;

    MetaPage(FileManager fileManager){
        super(fileManager);
    }

    @Override
    public ISlotDirItem createSlotDirItem(int index) {
        return new SlotDirItem(index);
    }

    @Override
    public IMetaRecord createMetaRecord(){
        return new MetaRecord();
    }

    @Override
    public IMetaRecord createMetaRecord(int columnLength, IDataType<?> dataType, int columnNameLength, String columnName) {
        return new MetaRecord(columnLength,dataType,columnNameLength,columnName);
    }

    @Override
    protected IRecord getRecord(int index) {
        return metaRecords.get(index);
    }

    @Override
    public void refresh() {
        actualBytes.position(0);
        refreshHeader();

        slotDirItems = parseSlotDirItems();

        metaRecords = parseMetaRecords();
    }

    @Override
    public List<IMetaRecord> getMetaRecords(){
        if(metaRecords == null){
            metaRecords = parseMetaRecords();
        }

        return metaRecords;
    }

    @Override
    public List<IMetaRecord> parseMetaRecords() {
        if(slotDirItems == null){
            slotDirItems = parseSlotDirItems();
        }

        List<IMetaRecord> metaRecords = new ArrayList<>(slotDirItems.size());
        for(ISlotDirItem slotDirItem : slotDirItems){
            IMetaRecord metaRecord = (IMetaRecord) slotDirItem.record();
            metaRecords.add(metaRecord);

            freeSpaceUntil -= slotDirItem.size();
            freeSpaceSize -= slotDirItem.size();
        }

        return metaRecords;
    }

    @Override
    public void setMetaRecords(List<IMetaRecord> metaRecords) {
        this.metaRecords = metaRecords;
    }

    @Override
    public PageType pageType() {
        return PageType.META;
    }

    private class SlotDirItem extends AbstractSlotDirItem {
        SlotDirItem(int index){
            super(index);
        }

        SlotDirItem(int index,int pos,int size){
            super(index,pos,size);
        }

        @Override
        public IMetaRecord record() {
            if(metaRecords != null && !metaRecords.isEmpty()){
                return metaRecords.get(index);
            }

            ByteBuffer buf = actualBytes;
            byte[] bytes = new byte[size];
            buf.position(pos);
            buf.get(bytes);

            IMetaRecord metaRecord = new MetaRecord();
            metaRecord.fromBytes(bytes);
            return metaRecord;
        }
    }

    private class MetaRecord implements IMetaRecord{
        int columnLength;

        IDataType<?> columnDataType;

        int columnNameLength;

        String columnName;

        MetaRecord(){

        }

        MetaRecord(int columnLength,IDataType<?> columnDataType,int columnNameLength,String columnName){
            this.columnLength = columnLength;
            this.columnDataType = columnDataType;
            this.columnNameLength = columnNameLength;
            this.columnName = columnName;
        }

        @Override
        public int columnLength() {
            return columnLength;
        }

        @Override
        public void columnLength(int columnLength) {
            this.columnLength = columnLength;
        }

        @Override
        public IDataType<?> columnDataType() {
            return columnDataType;
        }

        @Override
        public void columnDataType(IDataType<?> dataType) {
            this.columnDataType = dataType;
        }

        @Override
        public int columnNameLength() {
            return columnNameLength;
        }

        @Override
        public void columnNameLength(int columnNameLength) {
            this.columnNameLength = columnNameLength;
        }

        @Override
        public String columnName() {
            return columnName;
        }

        @Override
        public void columnName(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public byte[] toBytes() {
            final int length = 4 + columnNameLength;

            return toBytes(length);
        }

        @Override
        public byte[] toBytes(int length) {
            ByteBuffer buf = ByteBuffer.allocate(length);

            buf.put((byte) columnLength);
            buf.put((byte) (DataTypeRegistry.typeCode(columnDataType)));
            buf.putShort((short) columnNameLength);
            buf.put(columnName.getBytes(StandardCharsets.UTF_8));

            return buf.array();
        }

        @Override
        public void fromBytes(byte[] bytes) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);

            columnLength = buf.get() & 0xff;
            columnDataType = DataTypeRegistry.forTypeCode(buf.get() & 0xff);
            columnNameLength = buf.getShort() & 0xffff;
            byte[] columnNameBytes = new byte[columnNameLength];
            buf.get(columnNameBytes);
            columnName = new String(columnNameBytes,StandardCharsets.UTF_8);
        }

        @Override
        public int length() {
            return 4 + columnNameLength;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetaRecord that = (MetaRecord) o;
            return columnLength == that.columnLength && columnNameLength == that.columnNameLength && columnDataType == that.columnDataType && columnName.equals(that.columnName);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaPage that = (MetaPage) o;

        if(!headerEquals(that))
            return false;

        final int n = count();

        for(int i = 0;i < n;i++){
            if(!getSlotDirItems().get(i).equals(that.getSlotDirItems().get(i)))
                return false;

            if(!getMetaRecords().get(i).equals(that.getMetaRecords().get(i)))
                return false;
        }

        return true;
    }
}
