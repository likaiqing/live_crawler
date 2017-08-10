package com.pandatv.pojo;

import com.pandatv.common.Const;

/**
 * Created by likaiqing on 2017/2/23.
 */
public class IndexRec extends DetailAnchor {
    private String location;

    public String getLocation() {
        return null == location ? "" : location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String toString() {
//        return super.toString() + "&loc=" + this.getLocation();
        return super.toString() + Const.SEP + this.getLocation();
    }

}
