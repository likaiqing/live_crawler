package com.pandatv.pojo;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.tools.CommonTools;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by likaiqing on 2016/11/7.
 */
public class Anchor {
    private String rid;
    private String name;
    private String title;
    private String category;
    private String popularityStr;
    private int popularityNum;
    private String job;
    private String plat;
    private String game;
    private String url;

    public Anchor() {
        super();
    }

    public Anchor(String rid) {
        this.rid = rid;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getName() {
        return CommonTools.getFormatStr(name);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getTitle() {
        return CommonTools.getFormatStr(title);
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getPopularityStr() {
        return popularityStr;
    }

    public void setPopularityStr(String popularityStr) {
        this.popularityStr = popularityStr;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getPlat() {
        return plat;
    }

    public void setPlat(String plat) {
        this.plat = plat;
    }

    public String getGame() {
        return game;
    }

    public void setGame(String game) {
        this.game = game;
    }

    public int getPopularityNum() {
        return popularityNum;
    }

    public void setPopularityNum(int popularityNum) {
        this.popularityNum = popularityNum;
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj instanceof Anchor) {
            return ((Anchor) obj).getRid().equals(this.getRid());
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        String createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        StringBuffer sb = new StringBuffer();
//        return sb.append(this.rid).append(Const.SEP).append(this.getName()).append(Const.SEP).append(this.getTitle()).append(Const.SEP).append(this.getCategory()).append(Const.SEP).append(this.getPopularityStr()).append(Const.SEP).append(this.getPopularityNum()).append(Const.SEP).append(this.getJob()).append(Const.SEP).append(this.getPlat()).append(Const.SEP).append(this.getGame()).append(Const.SEP).append(createTime).append(Const.SEP).append(this.getUrl()).append(Const.SEP).append(PandaProcessor.getRandomStr()).toString();
        sb.append("&rid=").append(this.rid)
                .append("&nm=").append(PandaProcessor.encoder.encoder(this.getName()))
                .append("&tt=").append(PandaProcessor.encoder.encoder(this.getTitle()))
                .append("&cate=").append(PandaProcessor.encoder.encoder(this.getCategory()))
                .append("&pop_s=").append(PandaProcessor.encoder.encoder(this.getPopularityStr()))
                .append("&pop_n=").append(this.getPopularityNum())
                .append("&task=").append(this.getJob())
                .append("&plat=").append(this.getPlat())
                .append("&url_c=").append(this.getGame())
                .append("&c_time=").append(PandaProcessor.encoder.encoder(createTime))
                .append("&url=").append(this.getUrl())
                .append("&t_ran=").append(PandaProcessor.getRandomStr());
        return sb.toString();
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
