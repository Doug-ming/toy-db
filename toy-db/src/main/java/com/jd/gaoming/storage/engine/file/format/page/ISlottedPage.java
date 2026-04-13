package com.jd.gaoming.storage.engine.file.format.page;

import com.jd.gaoming.storage.engine.file.format.record.ISlotDirItem;
import java.util.List;

public interface ISlottedPage extends IPage{
    List<ISlotDirItem> getSlotDirItems();

    List<ISlotDirItem> parseSlotDirItems();

    ISlotDirItem createSlotDirItem(int i);

    void setSlotDirItems(List<ISlotDirItem> slotDirItems);

    int freeSpaceSize();

    void freeSpaceSize(int freeSpaceSize);

    int freeSpaceUntil();

    void freeSpaceUntil(int freeSpaceUntil);

    int freeSpaceBegin();

    void freeSpaceBegin(int freeSpaceBegin);
}
