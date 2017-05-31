package com.pandatv.pojo;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.tools.CommonTools;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by likaiqing on 2017/3/22.
 */
public class TwitchChannel {
    private int id;
    private String nickName;
    private String displayName;
    private String title;
    private String plat;
    private String game;
    private String broadcasterLan;
    private String language;
    private String registerTime;
    private String url;
    private int viewers;//在线人数
    private int viewsTol;//总观看人数
    private int followers;//总观看人数
    private String curUrl;
    private String task;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public String getDisplayName() {
        return CommonTools.getFormatStr(displayName);
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getTitle() {
        return CommonTools.getFormatStr(title);
    }

    public void setTitle(String title) {
        this.title = title;
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

    public String getBroadcasterLan() {
        return broadcasterLan;
    }

    public void setBroadcasterLan(String broadcasterLan) {
        this.broadcasterLan = broadcasterLan;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getViewers() {
        return viewers;
    }

    public void setViewers(int viewers) {
        this.viewers = viewers;
    }

    public int getViewsTol() {
        return viewsTol;
    }

    public void setViewsTol(int viewsTol) {
        this.viewsTol = viewsTol;
    }

    public int getFollowers() {
        return followers;
    }

    public void setFollowers(int followers) {
        this.followers = followers;
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj instanceof TwitchChannel) {
            return ((TwitchChannel) obj).getId() == (this.getId());
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        String createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        StringBuffer sb = new StringBuffer();
//        return sb.append(this.getId()).append(Const.SEP).append(this.getNickName()).append(Const.SEP).append(this.getDisplayName()).append(Const.SEP).append(this.getTitle()).append(Const.SEP).append(this.getPlat()).append(Const.SEP).append(this.getGame()).append(Const.SEP).append(this.getBroadcasterLan()).append(Const.SEP).append(this.getLanguage()).append(Const.SEP).append(this.getRegisterTime()).append(Const.SEP).append(this.getUrl()).append(Const.SEP).append(this.getViewers()).append(Const.SEP).append(this.getViewsTol()).append(Const.SEP).append(this.getFollowers()).append(Const.SEP).append(this.getCurUrl()).append(Const.SEP).append(this.getTask()).append(Const.SEP).append(createTime).append(Const.SEP).append(PandaProcessor.getRandomStr()).toString();
        sb.append("&id=").append(id)
                .append("&nick_nm=").append(PandaProcessor.encoder.encodeToString(this.getNickName().getBytes()))
                .append("&dis_nm=").append(PandaProcessor.encoder.encodeToString(this.getDisplayName().getBytes()))
                .append("&tt=").append(PandaProcessor.encoder.encodeToString(this.getTitle().getBytes()))
                .append("&plat=").append(this.getPlat())
                .append("&game=").append(PandaProcessor.encoder.encodeToString(this.getGame().getBytes()))
                .append("&broa_lan=").append(broadcasterLan)
                .append("&lan=").append(language)
                .append("&reg_time=").append(PandaProcessor.encoder.encodeToString(this.getRegisterTime().getBytes()))
                .append("&url=").append(PandaProcessor.encoder.encodeToString(this.getUrl().getBytes()))
                .append("&vies=").append(viewers)
                .append("&vie_tol=").append(viewsTol)
                .append("&fols=").append(followers)
                .append("&cur_u=").append(curUrl)
                .append("&task=").append(this.getTask())
                .append("&c_time=").append(PandaProcessor.encoder.encodeToString(createTime.getBytes()))
                .append("&t_ran=").append(PandaProcessor.getRandomStr());
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return 31 + (this.id + "").hashCode();
    }

    public String getTask() {
        return task;
    }

    public void setTask(String task) {
        this.task = task;
    }

    public String getCurUrl() {
        return curUrl;
    }

    public void setCurUrl(String curUrl) {
        this.curUrl = curUrl;
    }

    public String getRegisterTime() {
        return registerTime;
    }

    public void setRegisterTime(String registerTime) {
        this.registerTime = registerTime;
    }
}
