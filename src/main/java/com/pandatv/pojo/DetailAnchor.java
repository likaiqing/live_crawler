package com.pandatv.pojo;

import com.pandatv.common.Const;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by likaiqing on 2016/11/15.
 */
public class DetailAnchor {
    private String rid;
    private String name;
    private String title;
    private String category;//直播类型
    private int viewerNum;//人气
    private int followerNum;//关注数
    private String rank;//当前排名
    private String tag;//以逗号分隔
    private String weightStr;
    private long weightNum;//以g为单位,斗鱼才有,虎牙没有
    private String notice;//主播公告
    private String url;//主播公告

    public DetailAnchor(){
        super();
    }
    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public int getViewerNum() {
        return viewerNum;
    }

    public void setViewerNum(int viewerNum) {
        this.viewerNum = viewerNum;
    }

    public int getFollowerNum() {
        return followerNum;
    }

    public void setFollowerNum(int followerNum) {
        this.followerNum = followerNum;
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getWeightStr() {
        return weightStr;
    }

    public void setWeightStr(String weightStr) {
        this.weightStr = weightStr;
    }

    public long getWeightNum() {
        return weightNum;
    }

    public void setWeightNum(long weightNum) {
        this.weightNum = weightNum;
    }

    public String getNotice() {
        return notice;
    }

    public void setNotice(String notice) {
        this.notice = notice;
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj instanceof Anchor) {
            return ((Anchor) obj).getRid() == this.getRid();
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        String createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        StringBuffer sb = new StringBuffer();
        return "";
    }

    @Override
    public int hashCode() {
        return 31 + this.rid.hashCode();
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
