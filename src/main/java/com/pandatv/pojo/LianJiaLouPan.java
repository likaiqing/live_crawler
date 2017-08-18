package com.pandatv.pojo;

import com.pandatv.common.Const;

/**
 * Created by likaiqing on 2017/8/18.
 */
public class LianJiaLouPan extends AnJuKeLouPan {
    private String otherName;
    private String updateTimeStr;//数据更新时间
    private int daysAgo;//数据几天前更新
    private String hourseType;//房屋类型

    public int getDaysAgo() {
        return daysAgo;
    }

    public void setDaysAgo(int daysAgo) {
        this.daysAgo = daysAgo;
    }

    public String getHourseType() {
        return hourseType;
    }

    public void setHourseType(String hourseType) {
        this.hourseType = hourseType;
    }

    public String getOtherName() {
        return otherName;
    }

    public void setOtherName(String otherName) {
        this.otherName = otherName;
    }

    public String getUpdateTimeStr() {
        return updateTimeStr;
    }

    public void setUpdateTimeStr(String updateTimeStr) {
        this.updateTimeStr = updateTimeStr;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        return sb.append(super.toString()).append(Const.SEP)
                .append(this.getOtherName()).append(Const.SEP)
                .append(this.getUpdateTimeStr()).append(Const.SEP)
                .append(this.getDaysAgo()).append(Const.SEP)
                .append(this.getHourseType()).toString();
    }
}
