package com.pandatv.work;

import com.pandatv.pojo.DetailAnchor;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by likaiqing on 2016/11/22.
 */
public class DetailAnchorTest {
    private static Set<DetailAnchor> detailAnchorSet = new HashSet<>();
    public static void main(String[] args) {

        DetailAnchor detailAnchor = new DetailAnchor();
        detailAnchor.setRid("1234567");
        detailAnchor.setName("test");
        detailAnchor.setTitle("title");
        detailAnchorSet.add(detailAnchor);
        DetailAnchor d = new DetailAnchor("1234567");
        boolean contains = detailAnchorSet.contains(d);
        if (contains){
            System.out.println("contains");
        }
        Set<String> set = new HashSet<>();
        set.add("1234");
        boolean contains1 = set.contains("1234");
        if (contains1){
            System.out.println("contains");
        }
        boolean equals = detailAnchor.equals(d);
        if (equals){
            System.out.println("equals");
        }
    }
}
