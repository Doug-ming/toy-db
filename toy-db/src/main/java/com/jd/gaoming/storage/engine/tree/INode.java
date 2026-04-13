package com.jd.gaoming.storage.engine.tree;

import com.jd.gaoming.storage.engine.file.format.record.ILeafRecord;

public interface INode {
    long childPageNo();

    Comparable<?> key();

    ILeafRecord record();

    TreeNode child();

    void child(TreeNode treeNode);
}
