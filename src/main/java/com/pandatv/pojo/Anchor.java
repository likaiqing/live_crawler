package com.pandatv.pojo;

import com.pandatv.common.Const;

import java.util.Objects;

/**
 * Created by likaiqing on 2016/11/7.
 */
public class Anchor {
    private int rid;
    private String name;
    private String title;
    private String category;
    private String popularityStr;
    private int popularityNum;
    private String job;
    private String plat;
    private String game;

    public Anchor() {
        super();
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getRid() {
        return rid;
    }

    public void setRid(int rid) {
        this.rid = rid;
    }

    public String getTitle() {
        return title;
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

    public boolean equals(Object obj) {
        if (null==obj){
            return false;
        }
        if (obj instanceof Anchor){
            return ((Anchor) obj).getRid()==this.getRid();
        }else {
            return false;
        }
    }

    public String toString(){
        StringBuffer sb = new StringBuffer(this.rid);
        return sb.append(Const.SEP).append(this.name).append(Const.SEP).append(this.title).append(Const.SEP).append(this.category).append(Const.SEP).append(this.popularityStr).append(Const.SEP).append(this.popularityNum).append(Const.SEP).append(this.job).append(Const.SEP).append(this.plat).append(Const.SEP).append(this.game).toString();
    }
}
