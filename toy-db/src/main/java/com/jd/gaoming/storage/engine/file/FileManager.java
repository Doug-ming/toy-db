package com.jd.gaoming.storage.engine.file;

import com.jd.gaoming.storage.engine.annotations.Immutable;
import com.jd.gaoming.storage.engine.exceptions.DataAccessException;
import com.jd.gaoming.storage.engine.file.format.page.IPage;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

@Immutable
public final class FileManager {
    private final File dataFile;

    private final int pageSize;

    private final int fileHeaderSize;

    public FileManager(File dataFile,int fileHeaderSize,int pageSize) {
        this.dataFile = dataFile;
        this.fileHeaderSize = fileHeaderSize;
        this.pageSize = pageSize;
    }

    public ByteBuffer loadPageData(long pageNo){
        try{
            RandomAccessFile raf = new RandomAccessFile(dataFile,"rw");

            FileChannel fileChannel = null;

            try{
                fileChannel = raf.getChannel();
                long offset = pageNo * pageSize + fileHeaderSize;
                ByteBuffer buf = ByteBuffer.allocate(pageSize);

                fileChannel.read(buf,offset);
                buf.flip();

                return buf;
            }finally {
                if(fileChannel != null)
                    fileChannel.close();
            }
        }catch (IOException e){
            throw new DataAccessException(e);
        }
    }

    public void flushData(ByteBuffer buf,long offset){
        try{
            RandomAccessFile raf = new RandomAccessFile(dataFile,"rw");

            FileChannel fileChannel = null;

            try{
                fileChannel = raf.getChannel();

                buf.position(0);
                fileChannel.write(buf,offset);
            }finally {
                if(fileChannel != null)
                    fileChannel.close();
            }
        }catch (IOException e){
            throw new DataAccessException(e);
        }
    }

    public ByteBuffer readData(long offset,int length){
        try{
            RandomAccessFile raf = new RandomAccessFile(dataFile,"rw");

            FileChannel fileChannel = null;
            ByteBuffer buf = ByteBuffer.allocate(length);

            try{
                fileChannel = raf.getChannel();
                fileChannel.read(buf,offset);
            }finally {
                if(fileChannel != null)
                    fileChannel.close();
            }

            return buf;
        }catch (IOException e){
            throw new DataAccessException(e);
        }
    }

    public void flushPage(IPage page){
        final long pageNo = page.pageNo();
        final long offset = pageNo * pageSize + fileHeaderSize;

        flushData(page.internalBytes(),offset);
    }

    public long numberOfPage(){
        try{
            long totalPageLength = Files.size(dataFile.toPath()) - fileHeaderSize;

            if(totalPageLength < 0 || totalPageLength % pageSize != 0)
                throw new IllegalStateException(String.format("State of %s is illegal.",dataFile.getCanonicalPath()));

            return totalPageLength / pageSize;
        }catch (IOException e){
            throw new DataAccessException(e);
        }
    }
}
