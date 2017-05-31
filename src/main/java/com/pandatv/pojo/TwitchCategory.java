package com.pandatv.pojo;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.tools.CommonTools;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by likaiqing on 2017/3/21.
 */
public class TwitchCategory {
    private String plat;
    private int id;
    private int giantBombId;
    private String name;
    private int viewers;//观众数
    private int channels;//频道,即房间
    private String curUrl;
    private String nextUrl;
    private String task;
//    private String createTime;
//    private String taskRandom;

    public String getPlat() {
        return plat;
    }

    public void setPlat(String plat) {
        this.plat = plat;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getGiantBombId() {
        return giantBombId;
    }

    public void setGiantBombId(int giantBombId) {
        this.giantBombId = giantBombId;
    }

    public String getName() {
        return CommonTools.getFormatStr(name);
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getViewers() {
        return viewers;
    }

    public void setViewers(int viewers) {
        this.viewers = viewers;
    }

    public int getChannels() {
        return channels;
    }

    public void setChannels(int channels) {
        this.channels = channels;
    }

    public String getCurUrl() {
        return curUrl;
    }

    public void setCurUrl(String curUrl) {
        this.curUrl = curUrl;
    }

    public String getNextUrl() {
        return nextUrl;
    }

    public void setNextUrl(String nextUrl) {
        this.nextUrl = nextUrl;
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj instanceof TwitchCategory) {
            return ((TwitchCategory) obj).getId() == (this.getId());
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        String createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        StringBuffer sb = new StringBuffer();
//        return sb.append(this.getId()).append(Const.SEP).append(this.getGiantBombId()).append(Const.SEP).append(this.getPlat()).append(Const.SEP).append(this.getName()).append(Const.SEP).append(this.getViewers()).append(Const.SEP).append(this.getChannels()).append(Const.SEP).append(this.getCurUrl()).append(Const.SEP).append(this.getNextUrl()).append(Const.SEP).append(this.getTask()).append(Const.SEP).append(createTime).append(Const.SEP).append(PandaProcessor.getRandomStr()).toString();
        sb.append("&id=").append(id)
                .append("&g_b_id=").append(giantBombId)
                .append("&plat=").append(this.getPlat())
                .append("&nm=").append(PandaProcessor.encoder.encodeToString(this.getName().getBytes()))
                .append("&vies=").append(viewers)
                .append("&chas=").append(channels)
                .append("&cur_u=").append(curUrl)
                .append("&next_u=").append(nextUrl)
                .append("&task=").append(this.getTask())
                .append("&c_time=").append(PandaProcessor.encoder.encodeToString(createTime.getBytes()))
                .append("&t_ran=").append(PandaProcessor.getRandomStr());
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return 31 + (this.id+"").hashCode();
    }

    public String getTask() {
        return task;
    }

    public void setTask(String task) {
        this.task = task;
    }
}
