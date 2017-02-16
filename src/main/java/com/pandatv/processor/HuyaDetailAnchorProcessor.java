package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.MailTools;
import com.pandatv.tools.UnicodeTools;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by likaiqing on 2016/11/16.
 */
public class HuyaDetailAnchorProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HuyaDetailAnchorProcessor.class);
    private static String tmpUrl = "http://www.huya.com/cache.php?m=Live&do=ajaxAllLiveByPage&pageNum=1&page=";
    private static String tmpHostUrl = "http://www.huya.com/";
    private static Set<String> competitionLive = new HashSet<>();
    private static String competitionUrl = "http://www.huya.com/cache.php?m=HotRecApi&do=getLiveInfo&yyid=";
    private static int exCnt;
    private static Pattern isNotlivd = Pattern.compile("\"isNotLive\" : \"(\\d)\",");

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
                int newPage = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf('=') + 1)) + 1;
                if (all.size() > 0 || newPage<=1) {
                    page.putField("json", page.getJson().toString());
                    if (newPage < 800) {
                        String newUrl = this.tmpUrl + (newPage);
                        page.addTargetRequest(newUrl);
                    }
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
            } else if (curUrl.startsWith(competitionUrl)) {
                String json = UnicodeTools.unicodeToString(page.getJson().get());
                DetailAnchor detailAnchor = new DetailAnchor();
                String rid = null == page.getRequest().getExtra("rid") ? "" : page.getRequest().getExtra("rid").toString();
                detailAnchor.setRid(rid);
                detailAnchor.setName(JsonPath.read(json, "$.data." + rid + ".nick").toString());
                detailAnchor.setTitle(JsonPath.read(json, "$.data." + rid + ".introduction").toString());
                detailAnchor.setViewerNum(Integer.parseInt(JsonPath.read(json, "$.data." + rid + ".totalCount").toString()));
                detailAnchor.setFollowerNum(Integer.parseInt(JsonPath.read(json, "$.data." + rid + ".activityCount").toString()));
                detailAnchor.setCategorySec(JsonPath.read(json, "$.data." + rid + ".gameFullName").toString());
                detailAnchorObjs.add(detailAnchor);
                page.setSkip(true);
            } else {
                Object cycleTriedTimes = page.getRequest().getExtra("_cycle_tried_times");
                if (null != cycleTriedTimes && (int) cycleTriedTimes >= Const.CYCLERETRYTIMES - 1) {
                    timeOutUrl.append(curUrl).append(";");
                }
                Html html = page.getHtml();
                Matcher matcher = isNotlivd.matcher(html.toString());
                if (matcher.find() && matcher.group(1).equals("1")) {
                    return;
                }
                String rid = null == page.getRequest().getExtra("rid") ? "" : page.getRequest().getExtra("rid").toString();
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
                String viewerStr = html.xpath("//span[@class='host-spectator']/em/text()").get();
                if (!StringUtils.isEmpty(viewerStr) && viewerStr.contains(",")) {
                    viewerStr = viewerStr.replace(",", "");
                }
                String followerStr = html.xpath("//div[@id='activityCount']/text()").get();
                String tag = html.xpath("//span[@class='host-channel']/a/text()").all().toString();//逗号分隔
                String notice = html.xpath("//div[@class='notice-cont']/text()").get();
                DetailAnchor detailAnchor = new DetailAnchor();
                detailAnchor.setRid(rid);
                detailAnchor.setName(name);
                detailAnchor.setTitle(title);
                detailAnchor.setCategoryFir(categoryFir);
                detailAnchor.setCategorySec(categorySec);
                detailAnchor.setViewerNum(StringUtils.isEmpty(viewerStr) ? 0 : Integer.parseInt(viewerStr));
                detailAnchor.setFollowerNum(StringUtils.isEmpty(followerStr) ? 0 : Integer.parseInt(followerStr));
                detailAnchor.setTag(tag);
                detailAnchor.setNotice(notice);
                detailAnchor.setJob(job);
                detailAnchor.setUrl(curUrl);
                detailAnchorObjs.add(detailAnchor);
                page.setSkip(true);
            }
        } catch (Exception e) {
            if (competitionLive.contains(curUrl)) {
                String rid = curUrl.substring(curUrl.lastIndexOf("/") + 1);
                Request request = new Request(competitionUrl + rid).setPriority(5);
                request.putExtra("rid", rid);
                page.addTargetRequest(request);
            } else {
                failedUrl.append(curUrl + ";");
            }
            logger.info("process exception,url:{},html:{}" + curUrl, page.getHtml());
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail(Const.HUYAEXIT, "url: " + curUrl);
                System.exit(1);
            }
        }
    }

    @Override
    public Site getSite() {
        return this.site.setSleepTime(1);
    }

    public static void crawler(String[] args) {
        competitionLive.add("http://www.huya.com/1584989003");
        competitionLive.add("http://www.huya.com/1735596609");
        competitionLive.add("http://www.huya.com/1735597169");
        competitionLive.add("http://www.huya.com/1773588838");
        job = args[0];//
        date = args[1];
        hour = args[2];
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String firstUrl = "http://www.huya.com/cache.php?m=Live&do=ajaxAllLiveByPage&pageNum=1&page=1";
        String hivePaht = Const.COMPETITORDIR + "crawler_detail_anchor/" + date;
        Spider.create(new HuyaDetailAnchorProcessor()).thread(20).addUrl(firstUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        for (DetailAnchor detailAnchor : detailAnchorObjs) {
            detailAnchors.add(detailAnchor.toString());
        }
        logger.info("时间:" + date + " " + hour+"");
        CommonTools.writeAndMail(hivePaht, Const.HUYAFINISHDETAIL, detailAnchors);
    }
}
