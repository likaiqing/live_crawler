package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.LianJiaLouPan;
import com.pandatv.tools.PGTools;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by likaiqing on 2017/8/18.
 */
public class LianJiaProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(LianJiaProcessor.class);
    private static final String firUrl = "http://bj.fang.lianjia.com";
    private static final String firListUrlEndTmp = "/loupan/";
    private static final String otherListUrlTmp = "/loupan/pg";
    private static final String cityKeyParam = "city";
    private static final String indexKeyParam = "index";
    private static final String pageKeyParam = "page";
    private static Set<LianJiaLouPan> lianJiaList = new HashSet<>();
    private static int exCnt;

    private static final DateTimeFormatter stf = DateTimeFormat.forPattern("MMdd");

    public static void crawler(String[] args) {
        job = args[0];//lianjia
        date = args[1];//20161114
        hour = args[2];
        long start = System.currentTimeMillis();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("writeSuccess:" + writeSuccess);
                if (!writeSuccess) {
                    executeMapResults();
                }
            }
        }));
        Spider.create(new LianJiaProcessor()).thread(1).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString() + ",房间数:" + lianJiaList.size());
        executeMapResults();
    }

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            requests++;
            if (curUrl.equals(firUrl)) {
                /**
                 * 获取所有城市第一页url放入队列
                 */
                List<String> all = page.getHtml().xpath("//div[@class='fc-main clear']//div[@class='city-enum fl']/html()").all();
                for (String a : all) {
                    Html aHtml = new Html(a);
                    List<String> hrefs = aHtml.xpath("//a/@href").all();
                    List<String> citys = aHtml.xpath("//a/text()").all();
                    for (int i = 0; i < hrefs.size(); i++) {
                        page.addTargetRequest(new Request(hrefs.get(i) + firListUrlEndTmp).putExtra(cityKeyParam, citys.get(i)));
                    }
                    System.out.println();
                }
            } else if (curUrl.endsWith(firListUrlEndTmp)) {
                /**
                 * 处理第一页列表页
                 */
                Html html = page.getHtml();
                int totalPage = 2;
                try {
                    String pageData = html.xpath("//div[@class='page-box house-lst-page-box']/@page-data").get();
                    if (StringUtils.isEmpty(pageData)) {
                        try {
                            totalPage = Integer.parseInt(html.xpath("//div[@id='list-pagination']/@data-totalPage").get());
                        } catch (Exception e) {
                            List<String> pages = html.xpath("//div[@class='page_box']/a/text()").all();
                            if (pages.size() > 2) {
                                totalPage = Integer.parseInt(pages.get(pages.size() - 2));
                            }
                        }
                    } else {
                        String pageJson = pageData.substring(pageData.indexOf("{"), pageData.lastIndexOf("}") + 1);
                        JSONObject jsonObject = new JSONObject(pageJson);
                        totalPage = (int) jsonObject.get("totalPage");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info("解析页数出错,url:" + curUrl);
                    totalPage = Integer.parseInt(html.xpath("//div[@id='list-pagination']/@data-totalPage").get());
                }
                //将其他页url放入队列
                for (int i = 2; i <= totalPage; i++) {
                    page.addTargetRequest(new Request(new StringBuffer(curUrl).append("pg").append(i).append("/").toString()).putExtra(cityKeyParam, page.getRequest().getExtra(cityKeyParam)));
                }
                //获取详情页url放入队列
                parseListPage(curUrl, page);
            } else if (curUrl.contains(otherListUrlTmp)) {
                /**
                 * 处理非第一页的列表页
                 */
                //列表页获取详情页url放入队列
                parseListPage(curUrl, page);
            } else {
                /**
                 * 解析详情页数据
                 */

                Request request = page.getRequest();
                String id = curUrl.substring(curUrl.indexOf("/", curUrl.indexOf("loupan")) + 1, curUrl.lastIndexOf("/"));
                String city = request.getExtra(cityKeyParam).toString();
                String index = request.getExtra(indexKeyParam).toString();
                String pageNo = request.getExtra(pageKeyParam).toString();
                Html html = page.getHtml();
                List<String> as = html.xpath("//div[@class='breadcrumbs']/a/text()").all();
                String district = as.size() == 4 ? as.get(3).trim() : "";//区
                String check = html.xpath("//div[@class='box-left']/html()").get();
                LianJiaLouPan lianJiaLouPan = new LianJiaLouPan();
                lianJiaLouPan.setId(id);
                lianJiaLouPan.setCity(city);
                try {
                    lianJiaLouPan.setIndex(Integer.parseInt(index));
                    lianJiaLouPan.setPageNo(Integer.parseInt(pageNo));
                } catch (Exception e) {
                    e.printStackTrace();
                    lianJiaLouPan.setIndex(0);
                    lianJiaLouPan.setPageNo(0);
                    logger.error("解析下标或者页数出错,url:" + curUrl);
                }
                lianJiaLouPan.setDistrict(district);

                if (StringUtils.isEmpty(check)) {
                    setSpecialLianjia(lianJiaLouPan, html, curUrl);
                } else {
                    setNormalLianjia(lianJiaLouPan, html, curUrl);
                }
                lianJiaLouPan.setParDate(date);
//                lianJiaList.add(lianJiaLouPan.toString() + Const.TAB + date);
                lianJiaList.add(lianJiaLouPan);
            }
            page.setSkip(true);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("process error:curUrl:" + curUrl);
        }
    }

    private void setSpecialLianjia(LianJiaLouPan lianJiaLouPan, Html html, String curUrl) {
        String id = curUrl.substring(curUrl.indexOf("/", curUrl.indexOf("detail")) + 1, curUrl.lastIndexOf("/"));
        String name = html.xpath("//div[@class='title-row']/h1/text()").get();
        String otherName = html.xpath("//div[@class='alias-row']/span/text()").get();
        if (StringUtils.isNotEmpty(otherName)) {
            otherName = otherName.replace("别名：", "");
        }
        String status = html.xpath("//div[@class='title-row']/span[@class='status label']/text()").get();
        String hourseType = html.xpath("//div[@class='title-row']/span[@class='type label']/text()").get();
        String priceStr = html.xpath("//div[@class='price-row']/div[@class='left']/span[@class='row']/span[@class='num price']/text()").get();
        int price = 0;
        String unit = html.xpath("//div[@class='price-row']/div[@class='left']/span[@class='row']/span[@class='unit price']/text()").get();
        if (null != unit && unit.contains("元") && unit.contains("")) {
            unit = "square";
            try {
                price = Integer.parseInt(priceStr);
            } catch (Exception e) {
                e.printStackTrace();
                logger.info("setSpecialLianjia 解析价格出错,curlUrl:" + curUrl);
            }
        } else if (null != unit && unit.contains("万")) {
            unit = "suite";
            try {
                price = Integer.parseInt(priceStr) * 10000;
            } catch (Exception e) {
                e.printStackTrace();
                logger.info("setSpecialLianjia 解析价格出错,curlUrl:" + curUrl);
            }
        }
        String locationHtml = html.xpath("//div[@class='address-row']/table/tbody/tr[1]/html()").get();
        String location = "";
        if (StringUtils.isNotEmpty(locationHtml)) {
            String location1 = new Html(locationHtml).xpath("//a/text()").all().stream().reduce((a, b) -> a.trim() + " " + b.trim()).get();
            String location2 = new Html(locationHtml).xpath("//a[@class='address anchor-link']/span/text()").get();
            location = location1 + location2;
        }
        String openDate = html.xpath("//div[@class='address-row']/table/tbody/tr[3]/td[@class='info']/text()").get();
        if (StringUtils.isNotEmpty(openDate)) {
            openDate = openDate.replaceAll("年|月|日", "");
        }
        String closeDate = html.xpath("//div[@class='address-row']/table/tbody/tr[@id='submitTime']/td[2]/text()").get();
        if (StringUtils.isNotEmpty(closeDate)) {
            closeDate = closeDate.replaceAll("年|月|日", "");
        }
        String lastActionTitle = html.xpath("//div[@class='dynamicInfoEntry']/div[@class='di-title-line']/p[@class='di-title']/a/text()").get();
        String lastActionTime = html.xpath("//div[@class='dynamicInfoEntry']/div[@class='di-title-line']/p[@class='di-date']/text()").get();
        String lastActionContent = html.xpath("//div[@class='dynamicInfoEntry']/div[@class='di-content']/text()").get();
        if (StringUtils.isNotEmpty(lastActionTime)) {
            lastActionTime = lastActionTime.trim().replaceAll("-", "");
        }
        int daysAgo = 1;
        if (StringUtils.isNotEmpty(lastActionTime.trim())) {
            try {
                String trim = lastActionTime.trim().replaceAll("-","").substring(4);
                daysAgo = (new DateTime().dayOfYear().get()) - (stf.parseDateTime(trim).dayOfYear().get());
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        lianJiaLouPan.setId(id);
        lianJiaLouPan.setName(name);
        lianJiaLouPan.setStatus(status);
        lianJiaLouPan.setPriceText("");
        lianJiaLouPan.setPriceStr("");
        lianJiaLouPan.setOtherPriceStr("");
        lianJiaLouPan.setUnit(unit);
        lianJiaLouPan.setIntPrice(price);
        lianJiaLouPan.setIntOtherPrice(0);
        lianJiaLouPan.setAroundPriceStr("");
        lianJiaLouPan.setIntAroundPrice(0);
        lianJiaLouPan.setAdvantage("");
        lianJiaLouPan.setAjust("");
        lianJiaLouPan.setLocation(location);
        lianJiaLouPan.setOpenDate(openDate);
        lianJiaLouPan.setOpenDateFormat(openDate);
        lianJiaLouPan.setCloseDate("");
        lianJiaLouPan.setCloseDateFormat(closeDate);
        lianJiaLouPan.setLastActionTime(lastActionTime);
        lianJiaLouPan.setLastActionTitle(lastActionTitle);
        lianJiaLouPan.setLastActionContent(lastActionContent);
        lianJiaLouPan.setUrl(curUrl);
        lianJiaLouPan.setOtherName(otherName);
        lianJiaLouPan.setUpdateTimeStr(lastActionTime);
        lianJiaLouPan.setDaysAgo(daysAgo);
        lianJiaLouPan.setHourseType(hourseType);
    }

    private void setNormalLianjia(LianJiaLouPan lianJiaLouPan, Html html, String curUrl) {
        String status = html.xpath("//div[@class='box-left']/div[@class='box-left-top']/div[@class='name-box']/div[@class='state-div']/span[@class='state']/text()").get();
        String name = html.xpath("//div[@class='box-left']/div[@class='box-left-top']/div[@class='name-box']/a[@class='clear']/@title").get();
        int price = 0;
        try {
            price = Integer.parseInt(html.xpath("//div[@class='box-left']/div[@class='box-left-top']/p[@class='jiage']/span[@class='junjia']/text()").get().trim());
        } catch (Exception e) {
            logger.info("解析价格出错,url:" + curUrl);
            e.printStackTrace();
        }
        String unitStr = html.xpath("//div[@class='box-left']/div[@class='box-left-top']/p[@class='jiage']/span[@class='yuan']/text()").get();
        String unit = "square";
        if (StringUtils.isNotEmpty(unitStr) && unitStr.contains("万")) {
            price = price * 10000;
            unit = "suite";
        }
        if (StringUtils.isNotEmpty(unitStr) && unitStr.contains("套")) {
            unit = "suite";
        }
        String otherName = html.xpath("//div[@class='box-left']/div[@class='box-left-top']/p[@class='jiage']/span[@class='other-name']/text()").get();
        otherName = StringUtils.isNotEmpty(otherName) ? otherName.trim() : "";
        String updateTimeStr = html.xpath("//div[@class='box-left']/div[@class='box-left-top']/p[@class='update']/span/text()").get();
        int daysAgo = 1;
        if (StringUtils.isNotEmpty(updateTimeStr) && updateTimeStr.contains("月")) {
            String trim = updateTimeStr.substring(updateTimeStr.indexOf("：") + 1).replaceAll("年|月|日", "").trim();
            if (trim.length() == 4) {
                updateTimeStr = stf.print(stf.parseDateTime(trim));
                daysAgo = (new DateTime().dayOfYear().get()) - (stf.parseDateTime(trim).dayOfYear().get());
            }
        } else if (StringUtils.isNotEmpty(updateTimeStr) && updateTimeStr.contains("天前")) {
            daysAgo = Integer.parseInt(updateTimeStr.substring(updateTimeStr.indexOf("：") + 1).replace("天前", "").trim());
            updateTimeStr = stf.print(new DateTime().minusDays(daysAgo));
        } else if (StringUtils.isNotEmpty(updateTimeStr) && (updateTimeStr.contains("小时前") || updateTimeStr.contains("分钟前"))) {
            daysAgo = 1;
            updateTimeStr = stf.print(new DateTime().minusDays(1));
        }
        String hourseType = "";
        List<String> all = html.xpath("//div[@class='bottom-info']/p[@class='wu-type manager']/span/text()").all();
        if (null != all && all.size() == 2) {
            hourseType = all.get(1);
        } else {
            hourseType = html.xpath("//div[@class='bottom-info']/p[@class='wu-type ']/span/text()").all().get(1);
        }
        String location = html.xpath("//div[@class='bottom-info']/p[@class='where manager']/span/@title").get();
        if (StringUtils.isEmpty(location)) {
            location = html.xpath("//div[@class='bottom-info']/p[@class='where ']/span/@title").get();
        }
        String openDate = "";
        try {
            List<String> whenAll = html.xpath("//div[@class='bottom-info']/p[@class='when manager']/span/text()").all();
            if (null != whenAll && whenAll.size() == 2) {
                openDate = whenAll.get(1).replaceAll("年|月|日", "").trim();
            } else {
                openDate = html.xpath("//div[@class='bottom-info']/p[@class='when ']/span/text()").all().get(1).replaceAll("年|月|日", "").trim();
            }
        } catch (Exception e) {
            location.indexOf("解析开盘日期出错,url:" + curUrl);
            e.printStackTrace();
        }
        String lastActionTime = "";
        String lastActionTitle = "";
        String lastActionContent = "";
        try {
            String dynamic = html.xpath("//div[@class='dynamic-wrap-left pull-left']/div[@class='dynamic-wrap-block clearfix']/div[@class='dynamic-block-detail pull-right']/html()").get();
            Html dynamicHtml = new Html(dynamic);
            lastActionTitle = dynamicHtml.xpath("//div[@class='dongtai-title']/text()").get();
            lastActionContent = dynamicHtml.xpath("//a/text()").get().replace(" ", "").trim();
            lastActionTime = dynamicHtml.xpath("//div[@class='dynamic-detail-time']/span/text()").get().replaceAll("年|月|日", "").trim();
        } catch (Exception e) {
            location.indexOf("解析动态报错,url:" + curUrl);
            e.printStackTrace();
        }
        lianJiaLouPan.setName(name);
        lianJiaLouPan.setStatus(status);
        lianJiaLouPan.setPriceText("");
        lianJiaLouPan.setPriceStr("");
        lianJiaLouPan.setOtherPriceStr("");
        lianJiaLouPan.setUnit(unit);
        lianJiaLouPan.setIntPrice(price);
        lianJiaLouPan.setIntOtherPrice(0);
        lianJiaLouPan.setAroundPriceStr("");
        lianJiaLouPan.setIntAroundPrice(0);
        lianJiaLouPan.setAdvantage("");
        lianJiaLouPan.setAjust("");
        lianJiaLouPan.setLocation(location);
        lianJiaLouPan.setOpenDate(openDate);
        lianJiaLouPan.setOpenDateFormat(openDate);
        lianJiaLouPan.setCloseDate("");
        lianJiaLouPan.setCloseDateFormat("");
        lianJiaLouPan.setLastActionTime(lastActionTime);
        lianJiaLouPan.setLastActionTitle(lastActionTitle);
        lianJiaLouPan.setLastActionContent(lastActionContent);
        lianJiaLouPan.setUrl(curUrl);
        lianJiaLouPan.setOtherName(otherName);
        lianJiaLouPan.setUpdateTimeStr(updateTimeStr);
        lianJiaLouPan.setDaysAgo(daysAgo);
        lianJiaLouPan.setHourseType(hourseType);
    }

    /**
     * 解析列表页,获取详情url加入队列
     *
     * @param curUrl
     * @param page
     */
    private void parseListPage(String curUrl, Page page) {
        int curPageNo = 1;
        if (curUrl.contains("pg")) {
            curPageNo = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf("pg") + 2, curUrl.lastIndexOf("/")));
        }
        List<String> all = page.getHtml().xpath("//div[@class='list-wrap']/ul/li/div[@class='pic-panel']/html()").all();
        if (null == all || all.size() == 0) {
            all = page.getHtml().xpath("//div[@class='house-lst']/ul/li/div[@class='pic-panel']/html()").all();
        }
        for (String a : all) {
            Html aHtml = new Html(a);
            String detailUrl = aHtml.xpath("//a/@href").get();
            String index = aHtml.xpath("//a/@data-index").get();
            page.addTargetRequest(new Request(detailUrl).putExtra(cityKeyParam, page.getRequest().getExtra(cityKeyParam)).putExtra(indexKeyParam, index).putExtra(pageKeyParam, curPageNo));
        }

    }

    @Override
    public Site getSite() {
        return this.site.setSleepTime(120);
    }

    private static void executeMapResults() {
//        String dirFile = new StringBuffer("/home/likaiqing/data/lianjia/").append(date).append("_").append(hour).append("/").append(job).append("_").append(date).append("_").append(hour).append(randomStr).toString();
//        CommonTools.write2Local(dirFile, lianJiaList);
        PGTools.insertLianJiaLouPan(lianJiaList);
    }
}
