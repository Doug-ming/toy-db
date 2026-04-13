package com.jd.gaoming.storage.engine.serde;

import com.jd.gaoming.storage.engine.serde.interfaces.ToydbSerdes;
import com.jd.gaoming.storage.engine.table.IColumn;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

class DateTypeSerdes implements ToydbSerdes<Date> {
    @Override
    public byte[] serialize(Date value, IColumn column) {
        byte[] bytes = new byte[4];

        Calendar cal = Calendar.getInstance();
        cal.setTime(value);

        int centuryAndYear = cal.get(Calendar.YEAR);
        int century = centuryAndYear / 100;
        int year = centuryAndYear % 100;
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);


        bytes[0] = (byte) century;
        bytes[1] = (byte) year;
        bytes[2] = (byte) month;
        bytes[3] = (byte) day;

        return bytes;
    }

    @Override
    public void serialize(Date value, IColumn column, ByteBuffer buf) {
        buf.put(serialize(value,column));
    }

    @Override
    public Date deserialize(byte[] bytes, IColumn column) {
        int century = bytes[0];
        int year = bytes[1];
        int month = bytes[2] - 1;
        int day = bytes[3];

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR,century * 100 + year);
        cal.set(Calendar.MONTH,month);
        cal.set(Calendar.DAY_OF_MONTH,day);
        cal.set(Calendar.HOUR,0);
        cal.set(Calendar.MINUTE,0);
        cal.set(Calendar.SECOND,0);
        cal.set(Calendar.MILLISECOND,0);

        return cal.getTime();
    }
}
