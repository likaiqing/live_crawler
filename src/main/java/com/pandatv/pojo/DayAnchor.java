package com.pandatv.pojo;

/**
 * Created by likaiqing on 2016/12/8.
 */
public class DayAnchor {
    private static String rid;
    private static String name;
    private static String plat;
    private static long rec_times;

    public static String getRid() {
        return rid;
    }

    public static void setRid(String rid) {
        DayAnchor.rid = rid;
    }

    public static String getName() {
        return name;
    }

    public static void setName(String name) {
        DayAnchor.name = name;
    }

    public static String getPlat() {
        return plat;
    }

    public static void setPlat(String plat) {
        DayAnchor.plat = plat;
    }

    public static long getRec_times() {
        return rec_times;
    }

    public static void setRec_times(long rec_times) {
        DayAnchor.rec_times = rec_times;
    }
}
