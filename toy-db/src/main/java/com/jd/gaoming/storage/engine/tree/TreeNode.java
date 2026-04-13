package com.jd.gaoming.storage.engine.tree;

import com.jd.gaoming.storage.engine.file.format.page.IBranchPage;
import com.jd.gaoming.storage.engine.file.format.page.ILeafPage;
import com.jd.gaoming.storage.engine.file.format.page.IPage;
import com.jd.gaoming.storage.engine.file.format.page.ISlottedPage;

public interface TreeNode {
    boolean isLeaf();

    IBranchPage backendBranchPage();

    ILeafPage backendLeafPage();

    ISlottedPage backendPage();

    boolean overflow(int length);

    boolean underflow();

    int count();

    TreeNode child(int i);

    void fromPage(IPage page);

    void rewritePage();

    void rewriteAndFlushPage();
}
