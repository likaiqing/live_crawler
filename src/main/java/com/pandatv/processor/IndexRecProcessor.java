package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.IndexRec;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.MailTools;
import com.pandatv.tools.UnicodeTools;
import net.minidev.json.JSONArray;
import org.apache.commons.lang.StringUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by likaiqing on 2016/11/29.
 */
public class IndexRecProcessor extends PandaProcessor {
    private static List<String> douyuRecAnchors = new ArrayList<>();
    private static List<String> huyaRecAnchors = new ArrayList<>();
    private static String douyuDetailUrltmp = "http://open.douyucdn.cn/api/RoomApi/room/";
    private static String douyuIndex;
    private static String huyaIndex;
    private static String pandaIndex;
    private static int exCnt;
    private static final Map<String, IndexRec> map = new HashMap<>();
    private static final String pandaDetailPrefex = "http://www.panda.tv/";
    private static final String pandaFollowJsonPrefex = "http://www.panda.tv/room_followinfo?roomid=";
    private static final String pandaV2DetailJsonPrefex = "http://www.panda.tv/api_room_v2?roomid=";
    private static final Logger logger = LoggerFactory.getLogger(IndexRecProcessor.class);

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().toString();
        logger.info("process url:{}", curUrl);
        try {
            if (curUrl.equals(douyuIndex)) {
                executeDouyuIndex(page);
            } else if (curUrl.equals(huyaIndex)) {
                executeHuyaIndex(page);
            } else if (curUrl.equals(pandaIndex)) {
                executePandaIndex(page);
            } else if (curUrl.startsWith(pandaFollowJsonPrefex)) {
                executePandaFollow(page, curUrl);
            } else if (curUrl.startsWith(pandaV2DetailJsonPrefex)) {
                executePandaV2Json(page, curUrl);
            } else if (curUrl.startsWith(pandaDetailPrefex)) {
                executePandaDetail(page, curUrl);
            } else if (curUrl.startsWith(douyuDetailUrltmp)) {
                executeDouyRecDetail(page, curUrl);
            } else if (curUrl.startsWith(huyaIndex) && !curUrl.endsWith("/")) {
                executeHuyaRecDetail(page, curUrl);
            }
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail("斗鱼首页推荐", "url: " + curUrl);
                System.exit(1);
            }

        }
        page.setSkip(true);
    }

    private void executePandaV2Json(Page page, String curUrl) {
        String rid = page.getRequest().getExtra("rid").toString();
        IndexRec indexRec = map.get(rid);
        String detailJson = page.getJson().get();
        String name = JsonPath.read(detailJson, "$.data.hostinfo.name").toString();
        String weightStr = JsonPath.read(detailJson, "$.data.hostinfo.bamboos").toString();
        String title = JsonPath.read(detailJson, "$.data.roominfo.name").toString();
        String online = JsonPath.read(detailJson, "$.data.roominfo.person_num").toString();
        String category = JsonPath.read(detailJson, "$.data.roominfo.classification").toString();
        indexRec.setName(name);
        indexRec.setWeightNum(Long.parseLong(weightStr));
        indexRec.setTitle(title);
        indexRec.setViewerNum(Integer.parseInt(online));
        indexRec.setCategorySec(category);
        indexRec.setUrl(curUrl);
    }

    private void executePandaFollow(Page page, String curUrl) {
        String rid = page.getRequest().getExtra("rid").toString();
        IndexRec indexRec = map.get(rid);
        int follows = JsonPath.read(page.getJson().toString(), "$.data.fans");
        indexRec.setFollowerNum(follows);
    }

    private void executePandaDetail(Page page, String curUrl) {
        Elements scripts = page.getHtml().getDocument().getElementsByTag("script");
        boolean has = false;
        String rid = page.getRequest().getExtra("rid").toString();
        IndexRec indexRec = map.get(rid);
        for (Element script : scripts) {
            if (script.toString().contains("window._config_roominfo")) {
                has = true;
                String scrStr = script.toString();
                int hostinfoIndex = scrStr.indexOf("hostinfo");
                String hostInfoJson = scrStr.substring(hostinfoIndex + 11, scrStr.indexOf("},", hostinfoIndex) + 1);
                String name = UnicodeTools.unicodeToString(JsonPath.read(hostInfoJson, "$.name").toString());
                String weightStr = UnicodeTools.unicodeToString(JsonPath.read(hostInfoJson, "$.bamboos").toString());
                int roominfoIndex = scrStr.indexOf("roominfo", scrStr.indexOf("},"));
                int titleIndexStart = scrStr.indexOf("name", roominfoIndex);
                int titleIndexEnd = scrStr.indexOf("\",", titleIndexStart);
                String title = UnicodeTools.unicodeToString(scrStr.substring(titleIndexStart + 7, titleIndexEnd));
                int onlineIndexStart = scrStr.indexOf("person_num", roominfoIndex);
                int onlineIndexEnd = scrStr.indexOf("\",", onlineIndexStart);
                String online = scrStr.substring(onlineIndexStart + 13, onlineIndexEnd);
                int cateIndexStart = scrStr.indexOf("classification", roominfoIndex);
                int cateIndexEnd = scrStr.indexOf("\",", cateIndexStart);
                String cate = UnicodeTools.unicodeToString(scrStr.substring(cateIndexStart + 17, cateIndexEnd));
                indexRec.setName(name);
                indexRec.setTitle(title);
                indexRec.setCategorySec(cate);
                indexRec.setWeightNum(Long.parseLong(weightStr));
                indexRec.setViewerNum(Integer.parseInt(online));
                indexRec.setUrl(curUrl);
            }
        }
        if (!has) {//源码没有window._config_roominfo信息
            page.addTargetRequest(new Request(pandaV2DetailJsonPrefex + rid).putExtra("rid", rid));
        }
        page.addTargetRequest(new Request(pandaFollowJsonPrefex + rid).putExtra("rid", rid));
    }

    private void executePandaIndex(Page page) {
        List<String> all = page.getHtml().xpath("//div[@class='small-pic-container']/a[@class='small-pic-item-a']/@data-id").all();
        for (int i = 0; i < all.size(); i++) {
            //迭代方式location需加1
            IndexRec indexRec = new IndexRec();
            String rid = all.get(i);
            indexRec.setRid(rid);
            indexRec.setLocation((i + 1) + "");
            indexRec.setJob(Const.PANDAINDEXREC);
            map.put(rid, indexRec);
            page.addTargetRequest(new Request(pandaDetailPrefex + rid).putExtra("rid", rid));
        }
    }

    private void executeDouyuIndex(Page page) {
        List<String> all = page.getHtml().xpath("//div[@class='c-items']/ul/li/@data-id").all();
        for (int i = 0; i < all.size(); i++) {
            page.addTargetRequest(new Request(douyuDetailUrltmp + all.get(i)).putExtra("location", i + 1));
        }
    }

    private void executeHuyaRecDetail(Page page, String curUrl) {
        Object cycleTriedTimes = page.getRequest().getExtra("_cycle_tried_times");
        if (null != cycleTriedTimes && (int) cycleTriedTimes >= Const.CYCLERETRYTIMES - 1) {
            timeOutUrl.append(curUrl).append(";");
        }
        Integer location = (Integer) page.getRequest().getExtra("location");
        Html html = page.getHtml();
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
        IndexRec indexRec = new IndexRec();
        indexRec.setRid(rid);
        indexRec.setName(name);
        indexRec.setTitle(title);
        indexRec.setCategoryFir(categoryFir);
        indexRec.setCategorySec(categorySec);
        indexRec.setViewerNum(StringUtils.isEmpty(viewerStr) ? 0 : Integer.parseInt(viewerStr));
        indexRec.setFollowerNum(StringUtils.isEmpty(followerStr) ? 0 : Integer.parseInt(followerStr));
        indexRec.setTag(tag);
        indexRec.setNotice(notice);
        indexRec.setJob(Const.HUYAINDEXREC);
        indexRec.setUrl(curUrl);
        indexRec.setLocation(location + "");
        detailAnchors.add(indexRec.toString());
    }

    private void executeDouyRecDetail(Page page, String curUrl) {
        Object cycleTriedTimes = page.getRequest().getExtra("_cycle_tried_times");
        if (null != cycleTriedTimes && (int) cycleTriedTimes >= Const.CYCLERETRYTIMES - 1) {
            timeOutUrl.append(curUrl).append(";");
        }
        Integer location = (Integer) page.getRequest().getExtra("location");
        String json = page.getJson().get();
        String rid = JsonPath.read(json, "$.data.room_id");
        String name = JsonPath.read(json, "$.data.owner_name");
        String title = JsonPath.read(json, "$.data.room_name");
        String categorySec = JsonPath.read(json, "$.data.cate_name");
        int viewerStr = JsonPath.read(json, "$.data.online");
        String followerStr = JsonPath.read(json, "$.data.fans_num");
        String weightStr = JsonPath.read(json, "$.data.owner_weight");
        String lastStartTime = JsonPath.read(json, "$.data.start_time");
        IndexRec indexRec = new IndexRec();
        indexRec.setRid(rid);
        indexRec.setName(name);
        indexRec.setTitle(title);
        indexRec.setCategorySec(categorySec);
        indexRec.setViewerNum(viewerStr);
        indexRec.setFollowerNum(Integer.parseInt(followerStr));
        indexRec.setWeightNum(CommonTools.getDouyuWeight(weightStr));
        indexRec.setUrl(curUrl);
        indexRec.setLastStartTime(lastStartTime);
        indexRec.setJob(Const.DOUYUINDEXREC);
        indexRec.setLocation(location + "");
        detailAnchors.add(indexRec.toString());
    }

    private void executeHuyaIndex(Page page) {
        Elements js = page.getHtml().getDocument().getElementsByAttributeValue("data-fixed", "true");
        String slide = "";
        for (int i = 0; i < js.size(); i++) {
            Element element = js.get(i);
            if (element.toString().contains("var slides=")) {
                slide = element.toString();
                break;
            }
        }
        if (StringUtils.isEmpty(slide)) {
            return;
        }
        String recJson = slide.substring(slide.indexOf("var slides=") + 12, slide.indexOf("\"}];") + 4).trim().replace(";", "");
        JSONArray jsonArray = JsonPath.read(recJson, "$");
        for (Object rec : jsonArray) {
            String rid = JsonPath.read(rec, "$.privateHost").toString();
            int location = Integer.parseInt(JsonPath.read(rec, "$.recommendSite").toString());
            Request request = new Request(huyaIndex + rid);
            request.putExtra("rid", rid);
            request.putExtra("location", location);
            page.addTargetRequest(request);
        }

    }


    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        job = args[0];
        date = args[1];
        hour = args[2];
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        douyuIndex = "https://www.douyu.com/";
        huyaIndex = "http://www.huya.com/";
        pandaIndex = "http://www.panda.tv/";
        String hivePaht = Const.COMPETITORDIR + "crawler_indexrec_detail_anchor/" + date;
        Spider.create(new IndexRecProcessor()).thread(2).addUrl(douyuIndex, huyaIndex, pandaIndex).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        for (Map.Entry<String, IndexRec> entry : map.entrySet()) {
            detailAnchors.add(entry.getValue().toString());
        }
        CommonTools.writeAndMail(hivePaht, Const.INDEXRECEXIT, detailAnchors);
    }
}
