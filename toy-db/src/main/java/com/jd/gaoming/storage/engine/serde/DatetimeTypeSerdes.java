package com.jd.gaoming.storage.engine.serde;

import com.jd.gaoming.storage.engine.serde.interfaces.ToydbSerdes;
import com.jd.gaoming.storage.engine.table.IColumn;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

class DatetimeTypeSerdes implements ToydbSerdes<Date> {

    @Override
    public byte[] serialize(Date value, IColumn column) {
        byte[] bytes = new byte[7];

        Calendar cal = Calendar.getInstance();
        cal.setTime(value);

        int centuryAndYear = cal.get(Calendar.YEAR);
        int century = centuryAndYear / 100;
        int year = centuryAndYear % 100;
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR);
        int minute = cal.get(Calendar.MINUTE);
        int second = cal.get(Calendar.SECOND);

        bytes[0] = (byte) century;
        bytes[1] = (byte) year;
        bytes[2] = (byte) month;
        bytes[3] = (byte) day;
        bytes[4] = (byte) hour;
        bytes[5] = (byte) minute;
        bytes[6] = (byte) second;

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
        int hour = bytes[4];
        int minute = bytes[5];
        int second = bytes[6];

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR,century * 100 + year);
        cal.set(Calendar.MONTH,month);
        cal.set(Calendar.DAY_OF_MONTH,day);
        cal.set(Calendar.HOUR,hour);
        cal.set(Calendar.MINUTE,minute);
        cal.set(Calendar.SECOND,second);
        cal.set(Calendar.MILLISECOND,0);

        return cal.getTime();
    }
}
