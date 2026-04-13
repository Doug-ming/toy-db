package com.jd.gaoming.storage.engine;

import com.jd.gaoming.storage.engine.common.Constants;

public class Main {
    public static void main(String[] args) {
        byte[] bytes = Constants.MAGIC_BYTES;

        int magic = 0;

        magic += ((bytes[0] & 0xff) << 24);
        magic += ((bytes[1] & 0xff) << 16);
        magic += ((bytes[2] & 0xff) << 8);
        magic += (bytes[3] & 0xff);

        System.out.println(magic);
    }
}