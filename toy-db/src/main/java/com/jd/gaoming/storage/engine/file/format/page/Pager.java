package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.MetaPageCache;
import com.jd.gaoming.storage.engine.annotations.Nullable;
import com.jd.gaoming.storage.engine.common.Constants;
import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.file.format.record.IBranchRecord;
import com.jd.gaoming.storage.engine.file.format.record.IMetaRecord;
import com.jd.gaoming.storage.engine.file.format.record.ISlotDirItem;
import com.jd.gaoming.storage.engine.table.IColumn;
import com.jd.gaoming.storage.engine.table.IRow;
import com.jd.gaoming.storage.engine.table.ITableSchema;

import javax.swing.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Pager {
    public static IMetaPage createMetaPage(FileManager fileManager, long rootPageNo, ITableSchema tableSchema){
        MetaPage metaPage = new MetaPage(fileManager);

        metaPage.pageNo(0L);
        metaPage.flags(0);

        final int count = tableSchema.count();
        metaPage.count(count);

        metaPage.overflow(0);
        metaPage.checksum(0L);
        metaPage.multipurposePageNo(rootPageNo);
        metaPage.nextSiblingPageNo(0L);

        int freeSpaceUntil = Constants.PAGE_SIZE;

        List<ISlotDirItem> slotDirItems = new ArrayList<>();
        List<IMetaRecord> metaRecords = new ArrayList<>();

        for(int i = 0;i < tableSchema.count();i++){
            IColumn column = tableSchema.columns().get(i);
            IMetaRecord metaRecord = metaPage.createMetaRecord(column.length(),column.type(),column.name().length(),column.name());

            final int size = metaRecord.length();
            freeSpaceUntil -= size;
            final int pos = freeSpaceUntil;

            ISlotDirItem slotDirItem = metaPage.createSlotDirItem(i);
            slotDirItem.pos(pos);
            slotDirItem.size(size);
            slotDirItems.add(slotDirItem);

            metaRecords.add(metaRecord);
        }

        metaPage.slotDirItems = slotDirItems;
        metaPage.metaRecords = metaRecords;

        return metaPage;
    }

    public static IMetaPage restoreMetaPage(FileManager fileManager,String tableName){
        ByteBuffer buf = fileManager.loadPageData(0L);
        IMetaPage metaPage = new MetaPage(fileManager);
        metaPage.wrap(buf);

        return metaPage;
    }

    public static ISlottedPage restoreRootPage(FileManager fileManager,ITableSchema tableSchema,long rootPageNo) {
        ByteBuffer buf = fileManager.loadPageData(rootPageNo);
        int pageTypeMask = buf.get(4) & 0xff;

        ISlottedPage rootPage = null;
        if(pageTypeMask == PageType.LEAF.mask){
            rootPage = new LeafPage(fileManager,tableSchema);
        }else{
            rootPage = new BranchPage(fileManager,tableSchema.pkColumn());
        }

        rootPage.wrap(buf);

        return rootPage;
    }

    static class IndexItem{
        @Nullable
        final Comparable<?> key;
        final long childPageNo;

        IndexItem(Comparable<?> key,long childPageNo){
            this.key = key;
            this.childPageNo = childPageNo;
        }
    }

    static IBranchPage createMockBranchPage(FileManager fileManager,IColumn keyColumn,IndexItem... indexItems){
        BranchPage branchPage = new BranchPage(fileManager,keyColumn);

        branchPage.pageNo(1L);
        branchPage.flags(0);
        branchPage.count(indexItems.length - 1);
        branchPage.overflow(0);
        branchPage.checksum(0L);
        branchPage.multipurposePageNo(indexItems[indexItems.length - 1].childPageNo);
        branchPage.nextSiblingPageNo(0L);

        branchPage.slotDirItems = new ArrayList<>(branchPage.count() - 1);
        branchPage.branchRecords = new ArrayList<>(branchPage.count() - 1);

        int until = Constants.PAGE_SIZE;
        for(int i = 0;i < indexItems.length - 1;i++){
            IndexItem indexItem = indexItems[i];

            IBranchRecord branchRecord = branchPage.new BranchRecord(indexItem.childPageNo,indexItem.key);
            ISlotDirItem slotDirItem = branchPage.createSlotDirItem(i);
            final int recordLength = branchRecord.length();
            until -= recordLength;
            slotDirItem.pos(until);
            slotDirItem.size(recordLength);

            branchPage.slotDirItems.add(slotDirItem);
            branchPage.branchRecords.add(branchRecord);
        }

        return branchPage;
    }

    static ILeafPage createMockLeafPage(FileManager fileManager, ITableSchema tableSchema,IRow... rows){
        LeafPage leafPage = new LeafPage(fileManager,tableSchema);

        leafPage.pageNo(1L);
        leafPage.flags(0);
        leafPage.count(3);
        leafPage.overflow(0);
        leafPage.checksum(0L);
        leafPage.multipurposePageNo(0L);
        leafPage.nextSiblingPageNo(0L);

        leafPage.slotDirItems = new ArrayList<>();
        leafPage.leafRecords = new ArrayList<>();

        final int n = leafPage.count();
        int until = Constants.PAGE_SIZE;
        for(int i = 0;i < n;i++){
            IRow row = rows[i];
            LeafPage.LeafRecord leafRecord = leafPage.new LeafRecord();
            leafRecord.fromRow(row);

            ISlotDirItem slotDirItem = leafPage.createSlotDirItem(i);
            final int recordLength = leafRecord.length();
            until -= recordLength;
            slotDirItem.pos(until);
            slotDirItem.size(recordLength);

            leafPage.slotDirItems.add(slotDirItem);
            leafPage.leafRecords.add(leafRecord);
        }

        return leafPage;
    }

    public static ILeafPage createFreshLeafPage(FileManager fileManager,ITableSchema tableSchema,long pageNo){
        LeafPage leafPage = new LeafPage(fileManager,tableSchema);

        leafPage.pageNo(pageNo);
        leafPage.flags(0);
        leafPage.count(0);
        leafPage.overflow(0);
        leafPage.checksum(0L);
        leafPage.multipurposePageNo(0L);
        leafPage.nextSiblingPageNo(0L);

        leafPage.slotDirItems = new ArrayList<>();
        leafPage.leafRecords = new ArrayList<>();

        return leafPage;
    }

    public static IBranchPage createFreshBranchPage(FileManager fileManager,IColumn keyColumn,long pageNo){
        BranchPage branchPage = new BranchPage(fileManager,keyColumn);

        branchPage.pageNo(pageNo);
        branchPage.flags(0);
        branchPage.count(0);
        branchPage.overflow(0);
        branchPage.checksum(0L);
        branchPage.multipurposePageNo(0L);
        branchPage.nextSiblingPageNo(0L);

        branchPage.slotDirItems = new ArrayList<>();
        branchPage.branchRecords = new ArrayList<>();

        return branchPage;
    }

    public static IPage loadPageFromDisk(FileManager fileManager,ITableSchema tableSchema,long pageNo){
        ByteBuffer buf = fileManager.loadPageData(pageNo);

        PageType pageType = pageType(buf);
        IPage page = null;

        switch (pageType){
            case META : page = new MetaPage(fileManager);break;
            case BRANCH : page = new BranchPage(fileManager,tableSchema.pkColumn());break;
            case LEAF : page = new LeafPage(fileManager,tableSchema);break;
        }

        page.wrap(buf);

        return page;
    }

    public static PageType pageType(ByteBuffer buf){
        int pageTypeCode = buf.get(4) & 0xff;

        if(pageTypeCode == PageType.META.mask)
            return PageType.META;
        else if(pageTypeCode == PageType.BRANCH.mask)
            return PageType.BRANCH;
        else if(pageTypeCode == PageType.LEAF.mask)
            return PageType.LEAF;
        else
            throw new IllegalStateException(String.format("unknown page type code %d",pageTypeCode));
    }
}
