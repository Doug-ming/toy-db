package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.annotations.Nullable;
import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.file.format.AbstractSlotDirItem;
import com.jd.gaoming.storage.engine.file.format.record.IBranchRecord;
import com.jd.gaoming.storage.engine.file.format.record.IRecord;
import com.jd.gaoming.storage.engine.file.format.record.ISlotDirItem;
import com.jd.gaoming.storage.engine.serde.ToydbSerdesRegistry;
import com.jd.gaoming.storage.engine.serde.interfaces.ToydbSerdes;
import com.jd.gaoming.storage.engine.table.IColumn;
import com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry;
import com.jd.gaoming.storage.engine.table.datatypes.IDataType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class BranchPage extends AbstractSlottedPage implements IBranchPage{
    private final IColumn keyColumn;

    List<IBranchRecord> branchRecords;

    BranchPage(FileManager fileManager,IColumn keyColumn){
        super(fileManager);

        this.keyColumn = keyColumn;
    }

    @Override
    public PageType pageType() {
        return PageType.BRANCH;
    }

    @Override
    public void refresh() {
        refreshHeader();

        slotDirItems = parseSlotDirItems();

        branchRecords = parseBranchRecords();
    }

    @Override
    public List<IBranchRecord> parseBranchRecords() {
        if(slotDirItems == null){
            slotDirItems = parseSlotDirItems();
        }

        List<IBranchRecord> branchRecords = new ArrayList<>(slotDirItems.size());
        for(ISlotDirItem slotDirItem : slotDirItems){
            IBranchRecord branchRecord = (IBranchRecord) slotDirItem.record();
            branchRecords.add(branchRecord);

            freeSpaceUntil -= slotDirItem.size();
            freeSpaceSize -= slotDirItem.size();
        }

        return branchRecords;
    }

    @Override
    public void setBranchRecords(List<IBranchRecord> branchRecords) {
        this.branchRecords = branchRecords;
    }

    @Override
    public List<IBranchRecord> getBranchRecords() {
        if(branchRecords == null || branchRecords.isEmpty())
            branchRecords = parseBranchRecords();

        return branchRecords;
    }

    class SlotDirItem extends AbstractSlotDirItem {
        SlotDirItem(int index){
            super(index);
        }

        SlotDirItem(int index,int pos,int size){
            super(index,pos,size);
        }

        @Override
        public IBranchRecord record() {
            if(branchRecords != null && !branchRecords.isEmpty()){
                return branchRecords.get(index);
            }

            final int pos = this.pos;
            final int size = this.size;
            final ByteBuffer buf = actualBytes;

            buf.position(pos);
            byte[] bytes = new byte[size];
            buf.get(bytes);
            IBranchRecord branchRecord = new BranchRecord();
            branchRecord.fromBytes(bytes);

            return branchRecord;
        }
    }

    class BranchRecord implements IBranchRecord{
        @Nullable
        Comparable<?> key;

        long childPageNo;

        @Nullable
        byte[] keyBytes;

        BranchRecord(){

        }

        BranchRecord(long childPageNo,Comparable<?> key){
            this.childPageNo = childPageNo;
            this.key = key;
        }

        @Override
        public Comparable<?> key() {
            return key;
        }

        @Override
        public void key(Comparable<?> key) {
            this.key = key;
        }

        @Override
        public long childPageNo() {
            return childPageNo;
        }

        @Override
        public void childPageNo(long childPageNo) {
            this.childPageNo = childPageNo;
        }

        @Override
        public byte[] toBytes() {
            final int length = length();

            return toBytes(length);
        }

        @Override
        public byte[] toBytes(int length) {
            if(keyBytes == null){
                ByteBuffer buf = ByteBuffer.allocate(length);
                buf.putInt((int) childPageNo);
                ToydbSerdes serde = ToydbSerdesRegistry.getInstance(keyColumn.type());
                buf.put(serde.serialize(key,keyColumn));
                keyBytes = buf.array();
            }

            return keyBytes;
        }

        @Override
        public void fromBytes(byte[] bytes) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);

            childPageNo = buf.getInt() & 0xffffffffL;
            byte[] keyBytes = new byte[bytes.length - 4];
            buf.get(keyBytes);
            ToydbSerdes serde = ToydbSerdesRegistry.getInstance(keyColumn.type());
            key = (Comparable<?>) serde.deserialize(keyBytes,keyColumn);
        }

        @Override
        public int length() {
            IDataType<?> keyType = keyColumn.type();

            int keyLength;

            if(!keyType.lengthIsVariable() && keyType.hasPredefinedLength())
                keyLength = keyType.predefinedLength();
            else if(keyType == DataTypeRegistry.CHAR)
                keyLength = keyColumn.length();
            else if(keyType == DataTypeRegistry.VARCHAR) {
                if(keyBytes == null){
                    keyBytes = ((String) key).getBytes(StandardCharsets.UTF_8);
                }
                keyLength = keyBytes.length;
            }else {
                throw new IllegalStateException(String.format("Unable to calculate branch record length. Key type is %s",
                        keyType.typeName()));
            }

            return 4 + keyLength;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BranchRecord that = (BranchRecord) o;
            return childPageNo == that.childPageNo && key.equals(that.key);
        }
    }

    @Override
    public ISlotDirItem createSlotDirItem(int index) {
        return new SlotDirItem(index);
    }

    @Override
    public IBranchRecord createBranchRecord(){
        return new BranchRecord();
    }

    @Override
    public IBranchRecord createBranchRecord(long childPageNo,Comparable<?> key){
        return new BranchRecord(childPageNo,key);
    }

    @Override
    protected IRecord getRecord(int index) {
        return branchRecords.get(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BranchPage that = (BranchPage) o;

        if(!keyColumn.equals(that.keyColumn))
            return false;

        if(!headerEquals(that))
            return false;

        for(int i = 0;i < slotDirItems.size();i++){
            ISlotDirItem thisSlotDirItem = slotDirItems.get(i);
            ISlotDirItem thatSlotDirItem = that.slotDirItems.get(i);

            if(!thisSlotDirItem.equals(thatSlotDirItem))
                return false;
        }

        for(int i = 0;i < branchRecords.size();i++){
            IBranchRecord thisBranchRecord = branchRecords.get(i);
            IBranchRecord thatBranchRecord = that.branchRecords.get(i);

            if(!thisBranchRecord.equals(thatBranchRecord))
                return false;
        }

        return true;
    }

}
