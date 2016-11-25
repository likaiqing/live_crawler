package com.pandatv.work;

import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.HiveJDBCConnect;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by likaiqing on 2016/11/24.
 */
public class HiveTest {
    public static void write(String[] args) {
        HiveJDBCConnect hive = new HiveJDBCConnect();
        String hivePaht = "/tmp/hive/likaiqing/panda_realtime/panda_detail_anchor_crawler/2016112418";
        List<String> list = new ArrayList<>();
        list.add("hello");
        list.add("java");
        list.add("hive");
        hive.write2(hivePaht, list,"hivetest","2018");
        Set<DetailAnchor> anchors = new HashSet<>();
        DetailAnchor detailAnchor = new DetailAnchor("1234");
        DetailAnchor detailAnchor1 = new DetailAnchor("5678");
        DetailAnchor detailAnchor2 = new DetailAnchor("abcd");
        anchors.add(detailAnchor);
        anchors.add(detailAnchor1);
        anchors.add(detailAnchor2);
        hive.write2(hivePaht, anchors);
    }
}
