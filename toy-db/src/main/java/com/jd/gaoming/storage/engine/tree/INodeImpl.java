package com.jd.gaoming.storage.engine.tree;

import com.jd.gaoming.storage.engine.annotations.Nullable;
import com.jd.gaoming.storage.engine.file.format.record.ILeafRecord;

class INodeImpl implements INode{
    @Nullable
    Long childPageNo;

    @Nullable
    Comparable<?> key;

    @Nullable
    ILeafRecord record;

    @Nullable
    TreeNode treeNode;

    @Override
    public long childPageNo() {
        if(childPageNo == null){
            throw new UnsupportedOperationException("This inode represents an item in leaf node that has no childPageNo.");
        }

        return childPageNo;
    }

    @Override
    public Comparable<?> key() {
        return key;
    }

    @Override
    public ILeafRecord record() {
        if(record == null){
            throw new UnsupportedOperationException("This inode represents an item in branch node that has no record.");
        }

        return record;
    }

    @Override
    public @Nullable TreeNode child() {
        return treeNode;
    }

    @Override
    public void child(TreeNode treeNode) {
        this.treeNode = treeNode;
    }
}
