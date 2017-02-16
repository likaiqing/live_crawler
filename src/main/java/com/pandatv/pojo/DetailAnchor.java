package com.pandatv.pojo;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.tools.CommonTools;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by likaiqing on 2016/11/15.
 */
public class DetailAnchor {
    private String rid;
    private String name;
    private String title;
    private String categoryFir;//直播类型
    private String categorySec;//直播类型
    private int viewerNum;//人气
    private int followerNum;//关注数
    private String job;
    private String rank;//当前排名
    private String weightStr;
    private long weightNum;//以g为单位,斗鱼才有,虎牙没有
    private String tag;//以逗号分隔
    private String url;//主播公告
    private String notice;//主播公告
    private String lastStartTime;//主播公告

    public DetailAnchor() {
        super();
    }

    public DetailAnchor(String rid) {
        this.rid = rid;
    }

    public String getRid() {
        return CommonTools.getFormatStr(rid);
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getName() {
        return CommonTools.getFormatStr(name);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTitle() {
        return CommonTools.getFormatStr(title);
    }

    public void setTitle(String title) {
        this.title = title;
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
        return CommonTools.getFormatStr(notice);
    }

    public void setNotice(String notice) {
        this.notice = notice;
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj instanceof DetailAnchor) {
            return ((DetailAnchor) obj).getRid().equals(this.getRid());
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        String createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        StringBuffer sb = new StringBuffer();
        return sb.append(this.getRid()).append(Const.SEP).append(this.getName()).append(Const.SEP).append(this.getTitle()).append(Const.SEP).append(this.getCategoryFir()).append(Const.SEP).append(this.getCategorySec()).append(Const.SEP).append(this.getViewerNum()).append(Const.SEP).append(this.getFollowerNum()).append(Const.SEP).append(this.getJob()).append(Const.SEP).append(this.getRank()).append(Const.SEP).append(this.getWeightStr()).append(Const.SEP).append(this.getWeightNum()).append(Const.SEP).append(this.getTag()).append(Const.SEP).append(this.getUrl()).append(Const.SEP).append(createTime).append(Const.SEP).append(this.getNotice()).append(Const.SEP).append(this.getLastStartTime()).append(Const.SEP).append(PandaProcessor.getRandomStr()).toString();
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

    public String getCategoryFir() {
        return categoryFir;
    }

    public void setCategoryFir(String categoryFir) {
        this.categoryFir = categoryFir;
    }

    public String getCategorySec() {
        return categorySec;
    }

    public void setCategorySec(String categorySec) {
        this.categorySec = categorySec;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getJob() {
        return job;
    }

    public String getLastStartTime() {
        return lastStartTime;
    }

    public void setLastStartTime(String lastStartTime) {
        this.lastStartTime = lastStartTime;
    }

}
