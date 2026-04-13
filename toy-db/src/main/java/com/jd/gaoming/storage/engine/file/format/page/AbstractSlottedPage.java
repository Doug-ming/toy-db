package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.common.Constants;
import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.file.format.record.IRecord;
import com.jd.gaoming.storage.engine.file.format.record.ISlotDirItem;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractSlottedPage implements ISlottedPage {
    private Long pageNo;

    private Integer flags;

    private Integer count;

    private Integer overflow;

    private Long checksum;

    private Long multipurposePageNo;

    private Long nextSiblingPageNo;

    protected ByteBuffer actualBytes;

    protected final FileManager fileManager;

    protected boolean dirty;

    protected List<ISlotDirItem> slotDirItems;

    protected transient int freeSpaceBegin = Constants.PAGE_HEADER_SIZE;

    protected transient int freeSpaceUntil = Constants.PAGE_SIZE;

    protected transient int freeSpaceSize = freeSpaceUntil - freeSpaceBegin;

    @Override
    public int freeSpaceSize() {
        return freeSpaceSize;
    }

    @Override
    public void freeSpaceSize(int freeSpaceSize){
        this.freeSpaceSize = freeSpaceSize;
    }

    @Override
    public int freeSpaceBegin(){
        return freeSpaceBegin;
    }

    @Override
    public void freeSpaceBegin(int freeSpaceBegin){
        this.freeSpaceBegin = freeSpaceBegin;
    }

    @Override
    public int freeSpaceUntil(){
        return freeSpaceUntil;
    }

    @Override
    public void freeSpaceUntil(int freeSpaceUntil){
        this.freeSpaceUntil = freeSpaceUntil;
    }

    protected AbstractSlottedPage(FileManager fileManager){
        this.fileManager = fileManager;
    }

    protected abstract IRecord getRecord(int index);

    @Override
    public long pageNo(){
        if(pageNo == null){
            pageNo = (actualBytes.getInt(0) & 0xffffffffL);
        }

        return pageNo;
    }

    @Override
    public void pageNo(long pageNo){
        this.pageNo = pageNo;
    }

    @Override
    public int flags(){
        if(flags == null){
            flags = (actualBytes.get(5) & 0xff);
        }

        return flags;
    }

    @Override
    public void flags(int flags){
        this.flags = flags;
    }

    @Override
    public int count(){
        if(count == null){
            count = (actualBytes.getShort(6) & 0xffff);
        }

        return count;
    }

    @Override
    public void count(int c){
        count = c;
    }

    @Override
    public int overflow(){
        if(overflow == null){
            overflow = (actualBytes.getShort(8) & 0xffff);
        }

        return overflow;
    }

    @Override
    public void overflow(int overflow){
        this.overflow = overflow;
    }

    @Override
    public long checksum(){
        if(checksum == null){
            checksum = actualBytes.getLong(10);
        }

        return checksum;
    }

    @Override
    public void checksum(long checksum){
        this.checksum = checksum;
    }

    @Override
    public long multipurposePageNo(){
        if(multipurposePageNo == null){
            multipurposePageNo = actualBytes.getInt(18) & 0xffffffffL;
        }

        return multipurposePageNo;
    }

    @Override
    public void multipurposePageNo(long multipurposePageNo){
        this.multipurposePageNo = multipurposePageNo;
    }

    @Override
    public long nextSiblingPageNo(){
        if(nextSiblingPageNo == null){
            nextSiblingPageNo = actualBytes.getInt(22) & 0xffffffffL;
        }

        return nextSiblingPageNo;
    }

    @Override
    public void nextSiblingPageNo(long nextSiblingPageNo){
        this.nextSiblingPageNo = nextSiblingPageNo;
    }

    @Override
    public boolean isDirty(){
        return dirty;
    }

    @Override
    public void setDirty(boolean dirty){
        this.dirty = dirty;
    }

    @Override
    public void wrap(ByteBuffer buf,boolean eagerlyRefresh){
        actualBytes = buf;
        refresh();
    }

    @Override
    public void wrap(ByteBuffer buf){
        wrap(buf,true);
    }

    @Override
    public ByteBuffer internalBytes(){
        actualBytes.position(0);
        return actualBytes;
    }

    @Override
    public final void flush(){
        fileManager.flushPage(this);

        dirty = false;
    }

    protected final void rewritePageHeader(ByteBuffer buf){
        buf.putInt(0,(int) pageNo());
        buf.put(4,(byte) pageType().mask);
        buf.put(5,(byte) flags());
        buf.putShort(6,(short) count());
        buf.putShort(8,(short) overflow());
        buf.putLong(10,checksum());
        buf.putInt(18,(int) multipurposePageNo());
        buf.putInt(22,(int) nextSiblingPageNo());
    }

    @Override
    public void rewrite() {
        ByteBuffer buf = ByteBuffer.allocate(Constants.PAGE_SIZE);

        rewritePageHeader(buf);

        int pos = Constants.PAGE_HEADER_SIZE;

        for(int i = 0;i < count();i++){
            buf.position(pos);

            final ISlotDirItem slotDirItem = slotDirItems.get(i);
            buf.put(slotDirItem.toBytes());
            pos += slotDirItem.length();

            buf.position(slotDirItem.pos());
            final IRecord record = getRecord(i);
            buf.put(record.toBytes(slotDirItem.size()));
        }

        actualBytes = buf;
    }

    @Override
    public final List<ISlotDirItem> getSlotDirItems(){
        if(slotDirItems == null){
            slotDirItems = parseSlotDirItems();
        }

        return slotDirItems;
    }

    @Override
    public final List<ISlotDirItem> parseSlotDirItems() {
        ByteBuffer buf = actualBytes;

        final int n = count();
        List<ISlotDirItem> slotDirItems = new ArrayList<>(n);
        freeSpaceSize -= (n * Constants.SLOT_DIR_ITEM_SIZE);

        buf.position(Constants.PAGE_HEADER_SIZE);
        for(int i = 0;i < n;i++){
            ISlotDirItem slotDirItem = createSlotDirItem(i);
            byte[] bytes = new byte[slotDirItem.length()];
            buf.get(bytes);
            slotDirItem.fromBytes(bytes);
            slotDirItems.add(slotDirItem);
            freeSpaceBegin += slotDirItem.length();
        }

        return slotDirItems;
    }

    @Override
    public void setSlotDirItems(List<ISlotDirItem> slotDirItems){
        this.slotDirItems = slotDirItems;
    }

    protected final void refreshHeader(){
        pageNo = (actualBytes.getInt(0) & 0xffffffffL);

        flags = (actualBytes.get(5) & 0xff);

        count = (actualBytes.getShort(6) & 0xffff);

        overflow = (actualBytes.getShort(8) & 0xffff);

        checksum = actualBytes.getLong(10);

        multipurposePageNo = actualBytes.getInt(18) & 0xffffffffL;

        nextSiblingPageNo = actualBytes.getInt(22) & 0xffffffffL;
    }

    protected final boolean headerEquals(AbstractSlottedPage that){
        boolean b = (pageNo() == that.pageNo() && getClass() == that.getClass()
                && flags() == that.flags() && count() == that.count()
                && overflow() == that.overflow() && checksum() == that.checksum()
                && multipurposePageNo() == that.multipurposePageNo() && nextSiblingPageNo() == that.nextSiblingPageNo());

        if(!b)
            return false;

        byte[] bytes = new byte[Constants.PAGE_HEADER_SIZE];
        internalBytes().get(bytes,0,bytes.length);

        byte[] bytes1 = new byte[Constants.PAGE_HEADER_SIZE];
        that.internalBytes().get(bytes1,0,bytes1.length);

        return Arrays.equals(bytes,bytes1);
    }
}
