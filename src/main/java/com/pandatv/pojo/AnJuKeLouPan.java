package com.pandatv.pojo;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by likaiqing on 2017/8/17.
 */
public class AnJuKeLouPan {
    private String id;
    private String city;
    private String district;//区

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    private int index;//当前页第几个
    private int pageNo;//出现在第几页
    private String name;//小区或楼盘名称
    private String status;//售卖状态
    private String priceText;//文本形式价格
    private String priceStr;//字符串形式价格
    private String otherPriceStr;//字符串形式别墅等其他价格
    private String unit;//单位:每平米,每套
    private int intPrice;//整数形式价格
    private int intOtherPrice;//整数形式别墅等其他价格
    private String aroundPriceStr;
    private int intAroundPrice;//整数形式周边价格
    private String advantage;//优惠折扣
    private String ajust;//户型
    private String location;//具体地址
    private String openDate;//开盘日期
    private String openDateFormat;//格式化后yyyyMMdd开盘日期
    private String closeDate;//交房日期
    private String closeDateFormat;//格式化后yyyyMMdd交房日期
    private String lastActionTime;//最后动态日期
    private String lastActionTitle;//最后动态标题
    private String lastActionContent;//最后动态内容
    private String url;

    @Override
    public String toString() {
        String createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        StringBuffer sb = new StringBuffer();
        return sb.append(this.getId()).append(Const.TAB)
                .append(this.getCity()).append(Const.TAB)
                .append(this.getDistrict()).append(Const.TAB)
                .append(this.getIndex()).append(Const.TAB)
                .append(this.getPageNo()).append(Const.TAB)
                .append(this.getName()).append(Const.TAB)
                .append(this.getStatus()).append(Const.TAB)
                .append(this.getPriceText()).append(Const.TAB)
                .append(this.getPriceStr()).append(Const.TAB)
                .append(this.getOtherPriceStr()).append(Const.TAB)
                .append(this.getUnit()).append(Const.TAB)
                .append(this.getIntPrice()).append(Const.TAB)
                .append(this.getIntOtherPrice()).append(Const.TAB)
                .append(this.getAroundPriceStr()).append(Const.TAB)
                .append(this.getIntAroundPrice()).append(Const.TAB)
                .append(this.getAdvantage()).append(Const.TAB)
                .append(this.getAjust()).append(Const.TAB)
                .append(this.getLocation()).append(Const.TAB)
                .append(this.getOpenDate()).append(Const.TAB)
                .append(this.getOpenDateFormat()).append(Const.TAB)
                .append(this.getCloseDate()).append(Const.TAB)
                .append(this.getCloseDateFormat()).append(Const.TAB)
                .append(this.getLastActionTime()).append(Const.TAB)
                .append(this.getLastActionTitle()).append(Const.TAB)
                .append(this.getLastActionContent()).append(Const.TAB)
                .append(this.getUrl()).append(Const.TAB)
                .append(PandaProcessor.getRandomStr()).toString();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AnJuKeLouPan that = (AnJuKeLouPan) o;

        return url != null ? url.equals(that.url) : that.url == null;

    }

    @Override
    public int hashCode() {
        return url != null ? url.hashCode() : 0;
    }

    public String getAdvantage() {
        return advantage;
    }

    public void setAdvantage(String advantage) {
        this.advantage = advantage;
    }

    public String getAjust() {
        return ajust;
    }

    public void setAjust(String ajust) {
        this.ajust = ajust;
    }

    public String getAroundPriceStr() {
        return aroundPriceStr;
    }

    public void setAroundPriceStr(String aroundPriceStr) {
        this.aroundPriceStr = aroundPriceStr;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCloseDate() {
        return closeDate;
    }

    public void setCloseDate(String closeDate) {
        this.closeDate = closeDate;
    }

    public String getCloseDateFormat() {
        return closeDateFormat;
    }

    public void setCloseDateFormat(String closeDateFormat) {
        this.closeDateFormat = closeDateFormat;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public int getIntAroundPrice() {
        return intAroundPrice;
    }

    public void setIntAroundPrice(int intAroundPrice) {
        this.intAroundPrice = intAroundPrice;
    }

    public int getIntOtherPrice() {
        return intOtherPrice;
    }

    public void setIntOtherPrice(int intOtherPrice) {
        this.intOtherPrice = intOtherPrice;
    }

    public int getIntPrice() {
        return intPrice;
    }

    public void setIntPrice(int intPrice) {
        this.intPrice = intPrice;
    }

    public String getLastActionContent() {
        return lastActionContent;
    }

    public void setLastActionContent(String lastActionContent) {
        this.lastActionContent = lastActionContent;
    }

    public String getLastActionTime() {
        return lastActionTime;
    }

    public void setLastActionTime(String lastActionTime) {
        this.lastActionTime = lastActionTime;
    }

    public String getLastActionTitle() {
        return lastActionTitle;
    }

    public void setLastActionTitle(String lastActionTitle) {
        this.lastActionTitle = lastActionTitle;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getName() {
        return name;

    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOpenDate() {
        return openDate;
    }

    public void setOpenDate(String openDate) {
        this.openDate = openDate;
    }

    public String getOpenDateFormat() {
        return openDateFormat;
    }

    public void setOpenDateFormat(String openDateFormat) {
        this.openDateFormat = openDateFormat;
    }

    public String getOtherPriceStr() {
        return otherPriceStr;
    }

    public void setOtherPriceStr(String otherPriceStr) {
        this.otherPriceStr = otherPriceStr;
    }

    public String getPriceStr() {
        return priceStr;
    }

    public void setPriceStr(String priceStr) {
        this.priceStr = priceStr;
    }

    public String getPriceText() {
        return priceText;
    }

    public void setPriceText(String priceText) {
        this.priceText = priceText;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }
}
