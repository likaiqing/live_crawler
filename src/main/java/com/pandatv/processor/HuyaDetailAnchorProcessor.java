package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.mail.SendMail;
import com.pandatv.pipeline.HuyaDetailAnchorPipeline;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.HiveJDBCConnect;
import com.pandatv.tools.IOTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.selector.Html;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by likaiqing on 2016/11/16.
 */
public class HuyaDetailAnchorProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HuyaDetailAnchorProcessor.class);
    private static String tmpUrl = "http://www.huya.com/cache.php?m=Live&do=ajaxAllLiveByPage&pageNum=1&page=";
    private static String tmpHostUrl = "http://www.huya.com/";
    private static List<String> detailAnchors = new ArrayList<>();
    private static StringBuffer failedUrl = new StringBuffer("failedUrl:");
    private static String job = "";
    private static int index = 0;
    private static int exCnt;
    private static SendMail mail;

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().toString();
        logger.info("process url:{}", curUrl);
//        System.out.println("curUrl:"+curUrl);
        try {
            if (curUrl.startsWith(tmpUrl)) {
                List<String> all = page.getJson().jsonPath("$.data.list").all();
//                if (index++ > 10) {//测试使用,跑12页就行
//                    page.setSkip(true);
//                    return;
//                }
                if (all.size() > 0) {
                    page.putField("json", page.getJson().toString());
                    String newUrl = this.tmpUrl + (Integer.parseInt(curUrl.substring(curUrl.lastIndexOf('=') + 1)) + 1);
                    page.addTargetRequest(newUrl);
                    List<String> rooms = page.getJson().jsonPath("$.data.list").all();
                    for (String room : rooms) {
                        String rid = JsonPath.read(room, "$.privateHost");
                        Request request = new Request(tmpHostUrl + rid);
                        Map<String, Object> map = new HashMap<>();
                        map.put("rid", rid);
                        request.setExtras(map);
                        page.addTargetRequest(request);
                    }
                }
                page.setSkip(true);
            } else {
                Html html = page.getHtml();
                String rid = page.getRequest().getExtra("rid").toString();
                String name = html.xpath("//span[@class='host-name']/text()").get();
                String title = html.xpath("//h1[@class='host-title']/text()").get();
                String categoryFir = "";
                String categorySec = "";
                List<String> category = html.xpath("//span[@class='host-channel']/a/text()").all();
                if (category.size() == 2) {
                    categoryFir = category.get(0);
                    categorySec = category.get(1);
                } else if (category.size() == 1) {
                    categoryFir = category.get(0);
                    categorySec = category.get(0);
                }
//                String categorySec = html.xpath("//span[@class='host-channel']/a[2]/text()").get();
                String viewerStr = html.xpath("//span[@class='host-spectator']/em/text()").get().replace(",", "");
                String followerStr = html.xpath("//div[@id='activityCount']/text()").get();
                String tag = html.xpath("//span[@class='host-channel']/a/text()").all().toString();//逗号分隔
                String notice = html.xpath("//div[@class='notice-cont']/text()").get();
                DetailAnchor detailAnchor = new DetailAnchor();
                detailAnchor.setRid(rid);
                detailAnchor.setName(name);
                detailAnchor.setTitle(title);
                detailAnchor.setCategoryFir(categoryFir);
                detailAnchor.setCategorySec(categorySec);
                detailAnchor.setViewerNum(Integer.parseInt(viewerStr));
                detailAnchor.setFollowerNum(Integer.parseInt(followerStr));
                detailAnchor.setTag(tag);
                detailAnchor.setNotice(notice);
                detailAnchor.setJob(job);
                detailAnchor.setUrl(curUrl);
                detailAnchors.add(detailAnchor.toString());
//            if (detailAnchors.size()!=Const.WRITEBATCH){
//                page.setSkip(true);
//            }
                page.setSkip(true);
            }
        } catch (Exception e) {
            failedUrl.append(curUrl + ";");
//            mail.sendAlarmmail(Const.HUYAEXFLAG, "url: " + curUrl);
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                mail.sendAlarmmail(Const.HUYAEXIT, "url: " + curUrl);
                System.exit(1);
            }
        }
    }

    @Override
    public Site getSite() {
//        return this.site;
        return CommonTools.getAbuyunSite(site);
    }

    public static void crawler(String[] args) {
        mail = new SendMail("likaiqing@panda.tv", "");
        job = args[0];//
        String date = args[1];
        String hour = args[2];
        long s = System.currentTimeMillis();
        String firstUrl = "http://www.huya.com/cache.php?m=Live&do=ajaxAllLiveByPage&pageNum=1&page=1";
        HiveJDBCConnect hive = new HiveJDBCConnect();
        String hivePaht = Const.HIVEDIR + "panda_detail_anchor_crawler/" + date + hour;
        Spider.create(new HuyaDetailAnchorProcessor()).thread(13).addUrl(firstUrl).addPipeline(new HuyaDetailAnchorPipeline(detailAnchors, hive, hivePaht)).run();
        hive.write2(hivePaht, detailAnchors);
        long e = System.currentTimeMillis();
        mail.sendAlarmmail("虎牙爬取结束" + date + hour, failedUrl.toString());
        System.out.println("e-s:" + (e - s));
    }
}
