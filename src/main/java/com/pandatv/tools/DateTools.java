package com.pandatv.tools;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by likaiqing on 2016/11/25.
 */
public class DateTools {
    public static String getCurDate(){
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        return sf.format(new Date());
    }

    public static void main(String[] args) {
        String curDate = getCurDate();
        System.out.println(curDate);
    }
}
