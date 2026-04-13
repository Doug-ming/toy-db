package com.jd.gaoming.storage.engine.tree;

import com.jd.gaoming.storage.engine.MetaPageCache;
import com.jd.gaoming.storage.engine.annotations.Nullable;
import com.jd.gaoming.storage.engine.common.Constants;
import com.jd.gaoming.storage.engine.file.FileManager;
import com.jd.gaoming.storage.engine.file.format.page.*;
import com.jd.gaoming.storage.engine.file.format.record.IBranchRecord;
import com.jd.gaoming.storage.engine.file.format.record.ILeafRecord;
import com.jd.gaoming.storage.engine.file.format.record.ISlotDirItem;
import com.jd.gaoming.storage.engine.table.IRow;
import com.jd.gaoming.storage.engine.table.ITableSchema;
import java.util.ArrayList;
import java.util.List;

public class BTree {
    private TrNode root;

    private final FileManager fileManager;

    private final ITableSchema tableSchema;

    public final String tableName;

    private BTree(FileManager fileManager,String tableName,ITableSchema tableSchema){
        this.fileManager = fileManager;
        this.tableName = tableName;
        this.tableSchema = tableSchema;
    }

    public FileManager getFileManager() {
        return fileManager;
    }

    public ITableSchema getTableSchema() {
        return tableSchema;
    }

    class TrNode implements TreeNode{
        ISlottedPage backendPage;

        List<INode> inodes;

        boolean leaf;

        @Override
        public boolean isLeaf() {
            return leaf;
        }

        @Override
        public IBranchPage backendBranchPage() {
            if(!leaf)
                return (IBranchPage) backendPage;

            throw new UnsupportedOperationException("Leaf node has no backend branch page.");
        }

        @Override
        public ILeafPage backendLeafPage() {
            if(leaf)
                return (ILeafPage) backendPage;

            throw new UnsupportedOperationException("Internal node has no backend leaf page.");
        }

        @Override
        public ISlottedPage backendPage() {
            return backendPage;
        }

        @Override
        public boolean overflow(int recordLength) {
            return backendPage.freeSpaceSize() < recordLength;
        }

        @Override
        public boolean underflow() {
            return false;
        }

        @Override
        public int count() {
            return backendPage.count();
        }

        /**
         * Return the ith child of this node,possibly load from disk
         * */
        @Override
        public TreeNode child(int i) {
            TrNode childNode = (TrNode) inodes.get(i).child();

            if(childNode == null){
                childNode = diskRead(inodes.get(i).childPageNo());
                inodes.get(i).child(childNode);
            }

            return childNode;
        }

        /**
         * Actually transform page to tree node
         * */
        @Override
        public void fromPage(IPage page) {
            if(page instanceof IBranchPage){
                fromBranchPage((IBranchPage) page);
            }else if(page instanceof ILeafPage){
                fromLeafPage((ILeafPage) page);
            }else{
                throw new IllegalStateException("Only branch page or leaf page can be transformed to tree node.");
            }
        }

        /**
         * Actually transform tree node to page
         * */
        @Override
        public void rewritePage() {
            if(leaf){
                ILeafPage leafPage = Pager.createFreshLeafPage(fileManager,tableSchema,backendPage.pageNo());

                final int n = inodes.size();
                leafPage.count(n);

                int freeSpaceBegin = Constants.PAGE_HEADER_SIZE;
                int freeSpaceUntil = Constants.PAGE_SIZE;
                int freeSpaceSize = freeSpaceUntil - freeSpaceBegin;

                List<ISlotDirItem> slotDirItems = new ArrayList<>(n);
                List<ILeafRecord> leafRecords = new ArrayList<>(n);

                for(int i = 0;i < n;i++){
                    INodeImpl inode = (INodeImpl) inodes.get(i);

                    ILeafRecord leafRecord = inode.record;
                    final int recordLength = leafRecord.length();
                    freeSpaceUntil -= recordLength;

                    ISlotDirItem slotDirItem = backendPage.createSlotDirItem(i);
                    slotDirItem.pos(freeSpaceUntil);
                    slotDirItem.size(recordLength);
                    freeSpaceBegin += slotDirItem.length();

                    slotDirItems.add(slotDirItem);
                    leafRecords.add(leafRecord);

                    freeSpaceSize = freeSpaceUntil - freeSpaceBegin;
                }

                leafPage.setSlotDirItems(slotDirItems);
                leafPage.setLeafRecords(leafRecords);
                leafPage.freeSpaceSize(freeSpaceSize);
                leafPage.freeSpaceBegin(freeSpaceBegin);
                leafPage.freeSpaceUntil(freeSpaceUntil);

                leafPage.rewrite();

                backendPage = leafPage;
            }else{
                IBranchPage branchPage = Pager.createFreshBranchPage(fileManager,tableSchema.pkColumn(),backendPage.pageNo());
                final int n = inodes.size() - 1;
                branchPage.count(n);

                int freeSpaceBegin = Constants.PAGE_HEADER_SIZE;
                int freeSpaceUntil = Constants.PAGE_SIZE;
                int freeSpaceSize = freeSpaceUntil - freeSpaceBegin;

                List<ISlotDirItem> slotDirItems = new ArrayList<>(n);
                List<IBranchRecord> branchRecords = new ArrayList<>(n);

                for(int i = 0;i < n;i++){
                    INodeImpl inode = (INodeImpl) inodes.get(i);

                    IBranchRecord branchRecord = branchPage.createBranchRecord();
                    branchRecord.key(inode.key);
                    branchRecord.childPageNo(inode.childPageNo);
                    final int recordLength = branchRecord.length();
                    freeSpaceUntil -= recordLength;

                    ISlotDirItem slotDirItem = branchPage.createSlotDirItem(i);
                    slotDirItem.pos(freeSpaceUntil);
                    slotDirItem.size(recordLength);
                    freeSpaceBegin += slotDirItem.length();

                    slotDirItems.add(slotDirItem);
                    branchRecords.add(branchRecord);

                    freeSpaceSize = freeSpaceUntil - freeSpaceBegin;
                }

                branchPage.setSlotDirItems(slotDirItems);
                branchPage.setBranchRecords(branchRecords);
                branchPage.multipurposePageNo(inodes.get(n).childPageNo());//rightmost child pointer
                branchPage.freeSpaceBegin(freeSpaceBegin);
                branchPage.freeSpaceUntil(freeSpaceUntil);
                branchPage.freeSpaceSize(freeSpaceSize);

                branchPage.rewrite();
                backendPage = branchPage;
            }
        }

        @Override
        public void rewriteAndFlushPage() {
            rewritePage();

            backendPage.flush();
        }

        private void fromLeafPage(ILeafPage page) {
            backendPage = page;
            leaf = true;

            int n = page.count();
            List<ILeafRecord> leafRecordsInPage = page.getLeafRecords();

            inodes = new ArrayList<>(n);
            for(int i = 0;i < n;i++){
                INodeImpl inode = new INodeImpl();
                inode.record = leafRecordsInPage.get(i);
                inodes.add(inode);
            }
        }

        private void fromBranchPage(IBranchPage page) {
            backendPage = page;
            leaf = false;

            int n = page.count() + 1;
            List<IBranchRecord> branchRecordsInPage = page.getBranchRecords();

            inodes = new ArrayList<>(n);
            for(int i = 0;i < n - 1;i++){
                INodeImpl inode = new INodeImpl();
                inode.key = branchRecordsInPage.get(i).key();
                inode.childPageNo = branchRecordsInPage.get(i).childPageNo();
                inodes.add(inode);
            }

            INodeImpl inode = new INodeImpl();
            inode.childPageNo = page.multipurposePageNo();//rightmost child pointer
            inodes.add(inode);
        }
    }

    private TrNode diskRead(long pageNo){
        TrNode x = new TrNode();

        IPage page = Pager.loadPageFromDisk(fileManager,tableSchema,pageNo);

        x.fromPage(page);

        return x;
    }

    private TrNode allocateNode(boolean leaf){
        TrNode x = this.new TrNode();
        x.inodes = new ArrayList<>();
        x.leaf = leaf;

        long pageNo = fileManager.numberOfPage();

        if(leaf){
            //create an empty leaf page,only include page no
            x.backendPage = Pager.createFreshLeafPage(fileManager,tableSchema,pageNo);
        }else{
            //create fresh branch page
            x.backendPage = Pager.createFreshBranchPage(fileManager,tableSchema.pkColumn(),pageNo);
        }

        /*
        * do not allocate node when create table,lazily flush disk
        * also depends on single-threaded insert
        * */

        return x;
    }

    public static BTree createTree(FileManager fileManager,String tableName,ITableSchema tableSchema){
        BTree tree = new BTree(fileManager,tableName,tableSchema);

        tree.root = tree.allocateNode(true);

        IMetaPage metaPage = MetaPageCache.getMetaPage(tableName);
        metaPage.multipurposePageNo(1L);
        metaPage.rewrite();
        metaPage.flush();

        return tree;
    }

    public static BTree restoreTree(FileManager fileManager,String tableName){
        IMetaPage metaPage = MetaPageCache.getMetaPage(tableName);

        ITableSchema tableSchema = ITableSchema.restoreTableSchema(metaPage,0);

        long rootPageNo = metaPage.multipurposePageNo() & 0xffffffffL;
        ISlottedPage rootPage = Pager.restoreRootPage(fileManager,tableSchema,rootPageNo);

        BTree tree = new BTree(fileManager,tableName,tableSchema);
        TrNode x = tree.new TrNode();
        x.inodes = new ArrayList<>();
        x.leaf = rootPage.pageType() == PageType.LEAF;
        x.backendPage = rootPage;
        x.fromPage(rootPage);

        tree.root = x;

        return tree;
    }

    public ILeafRecord search(Comparable<?> key){
        return search(root,key);
    }

    private ILeafRecord search(TrNode x,Comparable<?> key){
        if(x.leaf){
            BinarySearchResult result = binarySearchForLeafNode(x.inodes,key);

            return result.pos < 0 ? null : x.inodes.get(result.pos).record();
        }else{
            int indexOfChild = binarySearchForInternalNode(x.inodes,key);
            TrNode child_i = (TrNode) x.child(indexOfChild);

            ILeafRecord leafRecord = search(child_i,key);
            x.inodes.get(indexOfChild).child(null);

            return leafRecord;
        }
    }

    public void insert(IRow row){
        SplitResult splitResult = insert(root,row);

        if(splitResult != null){//root split,tree grows taller
            TrNode x = allocateNode(false);

            x.inodes = new ArrayList<>();
            final Comparable<?> promotedKey = splitResult.promotedKey;
            final TrNode y = splitResult.newNode;

            INodeImpl inode = new INodeImpl();
            inode.key = promotedKey;
            inode.childPageNo = root.backendPage.pageNo();
            x.inodes.add(inode);

            inode = new INodeImpl();
            inode.key = null;
            inode.childPageNo = y.backendPage.pageNo();
            x.inodes.add(inode);

            x.rewriteAndFlushPage();

            root = x;

            //root pageNo change,rewrite meta page
            IMetaPage metaPage = MetaPageCache.getMetaPage(tableName);
            metaPage.multipurposePageNo(root.backendPage.pageNo());
            metaPage.rewrite();
            metaPage.flush();
        }
    }

    private @Nullable SplitResult insert(final TrNode x,IRow row){
        Comparable<?> key = row.getKey();

        if(x.leaf){//base case
            BinarySearchResult searchResult = binarySearchForLeafNode(x.inodes,key);
            final int insertPoint = searchResult.insertPoint;

            INodeImpl inode = new INodeImpl();
            ILeafRecord leafRecord = ((ILeafPage) x.backendPage).createLeafRecord();
            leafRecord.fromRow(row);
            inode.record = leafRecord;
            x.inodes.add(insertPoint,inode);

            //check overflow
            final int totalLength = Constants.SLOT_DIR_ITEM_SIZE + leafRecord.length();

            if(x.overflow(totalLength)){
                return splitTreeNode(x);
            }else{
                x.rewriteAndFlushPage();
                return null;
            }
        }else{
            final int i = binarySearchForInternalNode(x.inodes,key);

            TrNode child_i = (TrNode) x.child(i);//include possible load from disk
            SplitResult splitResult = insert(child_i,row);
            x.inodes.get(i).child(null);//help gc

            if(splitResult != null){
                Comparable<?> key_i = x.inodes.get(i).key();

                ((INodeImpl) x.inodes.get(i)).key = splitResult.promotedKey;

                INodeImpl inode = new INodeImpl();
                inode.key = key_i;
                inode.childPageNo = splitResult.newNode.backendPage().pageNo();
                x.inodes.add(i + 1,inode);

                x.rewritePage();

                //check overflow
                IBranchRecord branchRecord = x.backendBranchPage().createBranchRecord();
                branchRecord.key(splitResult.promotedKey);
                final int totalLength = Constants.SLOT_DIR_ITEM_SIZE + branchRecord.length();

                if(x.overflow(totalLength)){//overflow
                    return splitTreeNode(x);
                }else{
                    x.rewriteAndFlushPage();
                    return null;
                }
            }

            return null;
        }
    }

    private SplitResult splitLeafTreeNode(TrNode x){
        TrNode y = allocateNode(true);

        final int n = x.inodes.size();
        final int splitPoint = n >> 1;
        final Comparable<?> promotedKey = x.inodes.get(splitPoint).record().getKey();

        for(int j = 0;j < n - splitPoint;j++){
            y.inodes.add(j,x.inodes.get(splitPoint + j));
        }

        for(int j = n - 1;j >= splitPoint;j--){
            x.inodes.remove(j);
        }

        x.rewriteAndFlushPage();
        y.rewriteAndFlushPage();

        return new SplitResult(promotedKey,y);
    }

    private SplitResult splitInternalTreeNode(TrNode x){
        TrNode y = allocateNode(false);

        final int n = x.inodes.size();
        final int splitPoint = (n - 1) >> 1;
        final Comparable<?> promotedKey = x.inodes.get(splitPoint).key();

        for(int j = 0;j < n - splitPoint - 1;j++){
            y.inodes.add(x.inodes.get(j + splitPoint + 1));
        }

        for(int j = n - 1;j > splitPoint;j--){
            x.inodes.remove(j);
        }

        ((INodeImpl)x.inodes.get(splitPoint)).key = null;

        x.rewriteAndFlushPage();
        y.rewriteAndFlushPage();

        return new SplitResult(promotedKey,y);
    }

    private SplitResult splitTreeNode(TrNode x) {
        if(x.leaf){
            return splitLeafTreeNode(x);
        }else{
            return splitInternalTreeNode(x);
        }
    }

    /*
    * binary search for internal node,only decide which child to dive into
    * */
    private int binarySearchForInternalNode(List<INode> inodes,Comparable<?> key){
        int child = inodes.size() - 1;

        int begin = 0,until = inodes.size() - 1;

        while(begin < until){
            final int mid = (begin + until) >> 1;
            Comparable midKey = inodes.get(mid).key();

            if(midKey.compareTo(key) <= 0){
                begin = mid + 1;
            }else{
                child = mid;
                until = mid;
            }
        }

        return child;
    }

    /*
    * Binary search for leaf node,returns a binary search result.
    * If binarySearchResult.pos is negative,there is no record identified by the input key.
    * binarySearchResult.insertPoint points to the key that the incoming key should insert before.
    * */
    private BinarySearchResult binarySearchForLeafNode(List<INode> inodes,Comparable<?> key){
        int found = -1,insertPoint = inodes.size();

        int begin = 0,until = inodes.size();

        while(begin < until){
            final int mid = (begin + until) >> 1;
            Comparable midKey = inodes.get(mid).record().getKey();

            if(midKey.compareTo(key) <= 0){
                if(midKey.compareTo(key) == 0)
                    found = mid;

                begin = mid + 1;
            }else{
                insertPoint = mid;
                until = mid;
            }
        }

        return new BinarySearchResult(found,insertPoint);
    }

    private static class BinarySearchResult{
        final int pos;
        final int insertPoint;

        BinarySearchResult(int pos,int insertPoint){
            this.pos = pos;
            this.insertPoint = insertPoint;
        }
    }

    private static class SplitResult{
        final Comparable<?> promotedKey;

        final BTree.TrNode newNode;

        public SplitResult(Comparable<?> promotedKey,TrNode newNode) {
            this.promotedKey = promotedKey;
            this.newNode = newNode;
        }
    }
}
