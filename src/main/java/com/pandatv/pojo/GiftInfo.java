package com.pandatv.pojo;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by likaiqing on 2016/12/23.
 */
public class GiftInfo {
    private String plat;
    private String rId;
    private String category;
    private String giftId;
    private String name;
    private int type;
    private double price; //价格,单位元
    private int exp; //经验值,10=1元

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }


    public int getExp() {
        return exp;
    }

    public void setExp(int exp) {
        this.exp = exp;
    }

    @Override
    public int hashCode() {
        return this.getGiftId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj instanceof GiftInfo) {
            return this.getGiftId().equals(((GiftInfo) obj).getGiftId());
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        String createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        StringBuffer sb = new StringBuffer();
        return sb.append(this.getPlat()).append(Const.SEP).append(this.getCategory()).append(Const.SEP).append(this.getrId()).append(Const.SEP).append(this.getGiftId()).append(Const.SEP).append(this.getName()).append(Const.SEP).append(this.getType()).append(Const.SEP).append(this.getPrice()).append(Const.SEP).append(this.getExp()).append(Const.SEP).append(PandaProcessor.getRandomStr()).append(Const.SEP).append(createTime).toString();
    }

    public String getrId() {
        return rId;
    }

    public void setrId(String rId) {
        this.rId = rId;
    }

    public String getGiftId() {
        return giftId;
    }

    public void setGiftId(String giftId) {
        this.giftId = giftId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getPlat() {
        return plat;
    }

    public void setPlat(String plat) {
        this.plat = plat;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
