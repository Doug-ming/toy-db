package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.file.format.AbstractSlotDirItem;
import com.jd.gaoming.storage.engine.file.format.record.ILeafRecord;
import com.jd.gaoming.storage.engine.file.format.record.ISlotDirItem;
import com.jd.gaoming.storage.engine.serde.ToydbSerdesRegistry;
import com.jd.gaoming.storage.engine.serde.interfaces.ToydbSerdes;
import com.jd.gaoming.storage.engine.table.IColumn;
import com.jd.gaoming.storage.engine.table.IRow;
import com.jd.gaoming.storage.engine.table.ITableSchema;
import com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry;
import com.jd.gaoming.storage.engine.table.datatypes.IDataType;
import java.nio.ByteBuffer;
import java.util.*;

class LeafPage extends AbstractSlottedPage implements ILeafPage {
    private final ITableSchema tableSchema;

    final Map<Integer, IColumn> fixedLengthColumns;

    final Map<Integer, IColumn> varLengthColumns;

    List<ILeafRecord> leafRecords;

    LeafPage(FileManager fileManager, ITableSchema tableSchema) {
        super(fileManager);
        this.tableSchema = tableSchema;

        fixedLengthColumns = new LinkedHashMap<>();
        varLengthColumns = new LinkedHashMap<>();

        for (int i = 0; i < tableSchema.count(); i++) {
            IColumn column = tableSchema.columns().get(i);

            if (column.type().lengthIsVariable()) {
                varLengthColumns.put(i, column);
            } else {
                fixedLengthColumns.put(i, column);
            }
        }
    }

    @Override
    public ISlotDirItem createSlotDirItem(int index) {
        return new SlotDirItem(index);
    }

    @Override
    public ILeafRecord createLeafRecord(){
        return new LeafRecord();
    }

    @Override
    protected ILeafRecord getRecord(int index) {
        return leafRecords.get(index);
    }

    @Override
    public List<ILeafRecord> getLeafRecords() {
        if (leafRecords == null || leafRecords.isEmpty())
            leafRecords = parseLeafRecords();

        return leafRecords;
    }

    @Override
    public List<ILeafRecord> parseLeafRecords() {
        if (slotDirItems == null || slotDirItems.isEmpty())
            slotDirItems = parseSlotDirItems();

        List<ILeafRecord> leafRecords = new ArrayList<>(slotDirItems.size());
        for (ISlotDirItem slotDirItem : slotDirItems) {
            ILeafRecord leafRecord = (ILeafRecord) slotDirItem.record();
            leafRecords.add(leafRecord);

            freeSpaceUntil -= slotDirItem.size();
            freeSpaceSize -= slotDirItem.size();
        }

        return leafRecords;
    }

    @Override
    public void setLeafRecords(List<ILeafRecord> leafRecords) {
        this.leafRecords = leafRecords;
    }

    @Override
    public PageType pageType() {
        return PageType.LEAF;
    }

    @Override
    public void refresh() {
        refreshHeader();

        slotDirItems = parseSlotDirItems();

        leafRecords = parseLeafRecords();
    }

    class SlotDirItem extends AbstractSlotDirItem {

        SlotDirItem(int index) {
            super(index);
        }

        SlotDirItem(int index, int pos, int size) {
            super(index, pos, size);
        }

        @Override
        public ILeafRecord record() {
            if (leafRecords != null && !leafRecords.isEmpty())
                return leafRecords.get(index);

            final int pos = this.pos;
            final int size = this.size;
            final ByteBuffer buf = actualBytes;

            buf.position(pos);
            byte[] bytes = new byte[size];
            buf.get(bytes);
            ILeafRecord leafRecord = new LeafRecord();
            leafRecord.fromBytes(bytes);

            return leafRecord;
        }
    }

    private static class ValueHolder {
        final Object value;

        final byte[] bytes;

        ValueHolder(Object value, byte[] bytes) {
            this.value = value;
            this.bytes = bytes;
        }
    }

    class LeafRecord implements ILeafRecord {
        final ValueHolder[] valueHolders;

        int totalLength;

        LeafRecord() {
            valueHolders = new ValueHolder[tableSchema.count()];
        }

        @Override
        public IRow toRow() {
            IRow row = IRow.createEmptyRow(tableSchema);

            for (int i = 0; i < valueHolders.length; i++) {
                row.setValue(i, valueHolders[i].value);
            }

            return row;
        }

        @Override
        public void fromRow(IRow row) {
            tableSchema.columns().forEach(column -> {
                final int pos = column.position();
                final Object value = row.getValue(pos);

                ToydbSerdes serde = ToydbSerdesRegistry.getInstance(column.type());
                final byte[] valueBytes = serde.serialize(value, column);

                valueHolders[pos] = new ValueHolder(value, valueBytes);
            });
        }

        @Override
        public Comparable<?> getKey() {
            IColumn pkColumn = tableSchema.pkColumn();

            return (Comparable<?>) valueHolders[pkColumn.position()].value;
        }

        @Override
        public byte[] toBytes() {
            final int length = length();

            return toBytes(length);
        }

        @Override
        public byte[] toBytes(int length) {
            ByteBuffer buf = ByteBuffer.allocate(length);

            /*
             * first write fixed-length fields
             * */
            fixedLengthColumns.entrySet().forEach(e -> {
                IColumn column = e.getValue();
                ToydbSerdes serde = ToydbSerdesRegistry.getInstance(column.type());
                Object value = valueHolders[column.position()].value;
                buf.put(serde.serialize(value, column));
            });

            /*
             * write var-length fields
             * */
            List<byte[]> varLengthValues = new ArrayList<>(varLengthColumns.size());
            varLengthColumns.entrySet().forEach(e -> {
                IColumn column = e.getValue();
                ToydbSerdes serde = ToydbSerdesRegistry.getInstance(column.type());
                Object value = valueHolders[column.position()].value;
                byte[] bytes = serde.serialize(value, column);
                buf.putInt(bytes.length);
                varLengthValues.add(bytes);
            });

            varLengthValues.forEach(bytes -> buf.put(bytes));

            return buf.array();
        }

        @Override
        public void fromBytes(byte[] bytes) {
            totalLength = bytes.length;

            ByteBuffer buf = ByteBuffer.wrap(bytes);

            /*
             * process fixed-length fields
             * */
            for (Map.Entry<Integer, IColumn> entry : fixedLengthColumns.entrySet()) {
                final int pos = entry.getKey();
                final IColumn column = entry.getValue();

                final int size = fixedLengthFieldSize(column);
                byte[] valueBytes = new byte[size];
                buf.get(valueBytes);

                ToydbSerdes serde = ToydbSerdesRegistry.getInstance(column.type());
                final Object value = serde.deserialize(valueBytes, column);
                valueHolders[pos] = new ValueHolder(value, valueBytes);
            }

            /*
             * process var-length fields
             * */

            /*
             * first collect lengths of var-length fields
             * */
            List<Integer> sizeList = new ArrayList<>(varLengthColumns.size());
            varLengthColumns.entrySet().forEach(e -> sizeList.add(buf.getInt()));

            /*
             * parse values
             * */
            int i = 0;
            for (Map.Entry<Integer, IColumn> entry : varLengthColumns.entrySet()) {
                final IColumn column = entry.getValue();

                final int size = sizeList.get(i++);
                byte[] valueBytes = new byte[size];
                buf.get(valueBytes);

                ToydbSerdes serde = ToydbSerdesRegistry.getInstance(column.type());
                final Object value = serde.deserialize(valueBytes, column);
                valueHolders[column.position()] = new ValueHolder(value, valueBytes);
            }
        }

        private int fixedLengthFieldSize(IColumn column) {
            IDataType<?> type = column.type();

            if (type.hasPredefinedLength())
                return type.predefinedLength();
            else if (type == DataTypeRegistry.CHAR)
                return column.length();
            else {
                throw new IllegalStateException(String.format("Unable to calculate field length. Column name is %s",
                        column.name()));
            }
        }

        /**
         * Calculate total length of this record depending on full record state that originate from a row
         * */
        @Override
        public int length() {
            if (totalLength > 0)
                return totalLength;

            Arrays.stream(valueHolders).forEach(valueHolder -> {
                totalLength += (valueHolder.bytes.length);
            });

            totalLength += (varLengthColumns.size() * 4);

            return totalLength;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LeafRecord that = (LeafRecord) o;

            final int l1 = valueHolders.length;
            final int l2 = that.valueHolders.length;
            if(l1 != l2)
                return false;

            for(int i = 0;i < l1;i++){
                if(!deepEquals(valueHolders[i].bytes,that.valueHolders[i].bytes))
                    return false;
            }

            return true;
        }

        private boolean deepEquals(byte[] bytes_1,byte[] bytes_2){
            if(bytes_1.length != bytes_2.length)
                return false;

            for(int i = 0;i < bytes_1.length;i++){
                if(bytes_1[i] != bytes_2[i])
                    return false;
            }

            return true;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LeafPage that = (LeafPage) o;

        if(!headerEquals(that))
            return false;

        if(!tableSchema.equals(that.tableSchema))
            return false;

        final int n = count();
        for(int i = 0;i < n;i++){
            ISlotDirItem thisSlotDirItem = slotDirItems.get(i);
            ISlotDirItem thatSlotDirItem = that.slotDirItems.get(i);

            if(!thisSlotDirItem.equals(thatSlotDirItem))
                return false;
        }

        for(int i = 0;i < n;i++){
            ILeafRecord thisLeafRecord = leafRecords.get(i);
            ILeafRecord thatLeafRecord = that.leafRecords.get(i);

            if(!thisLeafRecord.equals(thatLeafRecord))
                return false;
        }

        return true;
    }

}