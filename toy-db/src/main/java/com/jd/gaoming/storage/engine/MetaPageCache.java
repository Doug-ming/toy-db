package com.jd.gaoming.storage.engine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.jd.gaoming.storage.engine.file.format.page.IMetaPage;

public class MetaPageCache {
    private static final ConcurrentMap<String,IMetaPage> META_PAGE_MAP = new ConcurrentHashMap<>();

    public static void addMetaPage(String tableName,IMetaPage metaPage){
        META_PAGE_MAP.put(tableName,metaPage);
    }

    public static IMetaPage getMetaPage(String tableName){
        IMetaPage metaPage = META_PAGE_MAP.get(tableName);

        if(metaPage == null)
            throw new IllegalArgumentException(String.format("Meta page not found.Please check table name %s",tableName));

        return metaPage;
    }
}
