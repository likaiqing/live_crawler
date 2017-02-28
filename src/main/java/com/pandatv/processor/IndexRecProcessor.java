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
import org.apache.commons.lang.StringEscapeUtils;
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

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static String zhanqiIndex;
    private static String longzhuIndex;
    private static String quanminIndex;
    private static String chushouindex;
    private static int exCnt;
    private static final Map<String, IndexRec> map = new HashMap<>();
    private static final String pandaDetailPrefex = "http://www.panda.tv/";
    private static final String pandaFollowJsonPrefex = "http://www.panda.tv/room_followinfo?roomid=";
    private static final String pandaV2DetailJsonPrefex = "http://www.panda.tv/api_room_v2?roomid=";
    private static final String longzhuDetailJsonPrefex = "http://roomapicdn.plu.cn/room/roomstatus?roomid=";
    private static final Logger logger = LoggerFactory.getLogger(IndexRecProcessor.class);
    private static final String quanminDetailUrlPre = "http://www.quanmin.tv/json/rooms/";
    private static final String quanminDetailUrlSuf = "/noinfo4.json";
    private static final String chushouDetailUrlPre = "https://chushou.tv/room/";
    private static final String chushouDetailUrlSuf = ".htm";
    private static String chushouPointUrlTmpPre = "https://chushou.tv/play-help/bang-guide-info.htm?roomId=";
    private static String chushouPointUrlTmpSuf = "&_=";//13位时间戳
    private static final SimpleDateFormat chushouFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

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
            } else if (curUrl.equals(zhanqiIndex)) {
                executeZhanqiIndex(page);
            } else if (curUrl.equals(longzhuIndex)) {
                executeLongzhuIndex(page);
            } else if (curUrl.startsWith("http://www.quanmin.tv/json/page/pc-index/info2.json")) {
                executeQuanminIndex(page);
            } else if (curUrl.equals(chushouindex)) {
                executeChushouIndex(page);
            } else if (curUrl.startsWith(chushouDetailUrlPre)) {
                executeChushouDetail(page, curUrl);
            } else if (curUrl.startsWith(chushouPointUrlTmpPre)) {
                executeChushouPoint(page, curUrl);
            } else if (curUrl.startsWith(quanminDetailUrlPre) && curUrl.endsWith(quanminDetailUrlSuf)) {
                executeQuanminDetail(page, curUrl);
            } else if (curUrl.startsWith(longzhuDetailJsonPrefex)) {
                executeLongzhuDetail(page, curUrl);
            } else if (curUrl.startsWith(zhanqiIndex)) {
                executeZhanqiDetail(page, curUrl);
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
            logger.error("execute faild,url:" + curUrl);
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail("斗鱼首页推荐", "url: " + curUrl);
                System.exit(1);
            }

        }
        page.setSkip(true);
    }

    private void executeChushouPoint(Page page, String curUrl) {
        String json = page.getJson().toString();
        Integer point = JsonPath.read(json, "$.data.current.point");
        Long lastDate = JsonPath.read(json, "$.data.last.date");
        String rid = page.getRequest().getExtra("rid").toString();
        IndexRec indexRec = map.get(rid + Const.CHUSHOUINDEXREC);
        indexRec.setWeightNum(null == point ? 0 : point);
        indexRec.setLastStartTime(null == lastDate ? null : chushouFormat.format(lastDate / 1000));
    }

    private void executeChushouDetail(Page page, String curUrl) {
        String rid = page.getRequest().getExtra("rid").toString();
        IndexRec indexRec = map.get(rid + Const.CHUSHOUINDEXREC);
        Html html = page.getHtml();
        String title = StringEscapeUtils.unescapeHtml(html.xpath("//p[@class='zb_player_gamedesc']/@title").get());
        String name = StringEscapeUtils.unescapeHtml(html.xpath("//span[@id='sp1']/@title").get());
        String category = StringEscapeUtils.unescapeHtml(html.xpath("//a[@id='sp2']/@title").get());
        String online = StringEscapeUtils.unescapeHtml(html.xpath("//span[@class='onlineCount']/text()").get());
        String follow = StringEscapeUtils.unescapeHtml(html.xpath("//div[@class='zb_attention_left']/@data-subscribercount").get());
        indexRec.setName(name);
        indexRec.setTitle(title);
        indexRec.setViewerNum(Integer.parseInt(online));
        indexRec.setFollowerNum(Integer.parseInt(follow));
        indexRec.setCategorySec(category);
        indexRec.setUrl(curUrl);
    }

    private void executeChushouIndex(Page page) {
        List<String> scripts = page.getHtml().xpath("//script").all();
        for (int i = 0; i < scripts.size(); i++) {
            String script = scripts.get(i);
            if (script.contains("statserver")) {
                Pattern compile = Pattern.compile("poster\\['targetKey'\\]=\"\\d+\";");
                Matcher matcher = compile.matcher(script);
                int index = 0;
                while (matcher.find()) {
                    String targetStr = matcher.group(0);
                    String targetKey = targetStr.substring(targetStr.indexOf("=") + 2, targetStr.lastIndexOf("\";"));
                    page.addTargetRequest(new Request(chushouDetailUrlPre + targetKey + chushouDetailUrlSuf).putExtra("rid", targetKey));
                    page.addTargetRequest(new Request(chushouPointUrlTmpPre + targetKey + chushouPointUrlTmpSuf + new Date().getTime()).putExtra("rid", targetKey));
                    page.addTargetRequest("");
                    IndexRec indexRec = new IndexRec();
                    indexRec.setRid(targetKey);
                    indexRec.setLocation((++index) + "");
                    indexRec.setJob(Const.CHUSHOUINDEXREC);
                    map.put(targetKey + Const.CHUSHOUINDEXREC, indexRec);
                }
            }
        }
    }

    private void executeQuanminDetail(Page page, String curUrl) {
        String location = page.getRequest().getExtra("location").toString();
        String json = page.getJson().toString();
        int rid = JsonPath.read(json, "$.uid");
        String name = JsonPath.read(json, "$.nick");
        String title = JsonPath.read(json, "$.title");
        String category = JsonPath.read(json, "$.category_name");
        int online = JsonPath.read(json, "$.view");
        int follow = JsonPath.read(json, "$.follow");
        int weight = JsonPath.read(json, "$.weight");//统计的时候需要除以100
        String lastStartTime = JsonPath.read(json, "$.play_at");//substring(0,16)
        IndexRec indexRec = new IndexRec();
        indexRec.setRid(rid + "");
        indexRec.setName(name);
        indexRec.setTitle(title);
        indexRec.setCategorySec(category);
        indexRec.setViewerNum(online);
        indexRec.setFollowerNum(follow);
        indexRec.setWeightNum((int) (weight / 100));
        indexRec.setLastStartTime(lastStartTime.substring(0, 16));
        indexRec.setJob(Const.QUANMININDEXREC);
        indexRec.setUrl(curUrl);
        indexRec.setLocation(location);
        detailAnchors.add(indexRec.toString());
    }

    private void executeQuanminIndex(Page page) {
        JSONArray livelist = JsonPath.read(page.getJson().toString(), "$.backpic.livelist");
        for (int i = 0; i < livelist.size(); i++) {
            Integer room = JsonPath.read(livelist.get(i), "$.room");
            page.addTargetRequest(new Request(quanminDetailUrlPre + room + quanminDetailUrlSuf).putExtra("location", i + 1));
        }
    }

    private void executeLongzhuDetail(Page page, String curUrl) {
        String rid = page.getRequest().getExtra("rid").toString();
        String name = page.getRequest().getExtra("name").toString();
        String location = page.getRequest().getExtra("location").toString();
        String json = page.getJson().toString();
        String title = JsonPath.read(json, "$.Broadcast.Title").toString();
        String category = JsonPath.read(json, "$.Broadcast.GameName").toString();
        int online = JsonPath.read(json, "$.OnlineCount");
        int flowerCount = JsonPath.read(json, "$.FlowerCount");
        int follow = JsonPath.read(json, "$.RoomSubscribeCount");
        IndexRec indexRec = new IndexRec();
        indexRec.setRid(rid);
        indexRec.setName(name);
        indexRec.setTitle(title);
        indexRec.setLocation(location);
        indexRec.setJob(Const.LONGZHUINDEXREC);
        indexRec.setViewerNum(online);
        indexRec.setFollowerNum(follow);
        indexRec.setWeightNum(flowerCount);
        indexRec.setUrl(curUrl);
        indexRec.setCategorySec(category);
        detailAnchors.add(indexRec.toString());
    }

    private void executeLongzhuIndex(Page page) {
        List<String> rids = page.getHtml().xpath("//div[@class='side']/ul/li/@data-domain").all();
        List<String> detailJsons = page.getHtml().xpath("//div[@class='side']/ul/li/@data-roomid").all();
        List<String> names = page.getHtml().xpath("//div[@class='side']/ul/li/@data-user").all();
        for (int i = 0; i < rids.size(); i++) {
            page.addTargetRequest(new Request(longzhuDetailJsonPrefex + detailJsons.get(i)).putExtra("rid", rids.get(i)).putExtra("name", names.get(i)).putExtra("location", i + 1));//url的标示作为rid
        }
    }

    private void executeZhanqiDetail(Page page, String curUrl) {
        List<String> allScript = page.getHtml().xpath("//script").all();
        IndexRec indexRec = new IndexRec();
        String location = page.getRequest().getExtra("location").toString();
        for (String script : allScript) {
            if (script.contains("window.oPageConfig.oRoom")) {
                String json = script.substring(script.indexOf("{"), script.lastIndexOf("}") + 1);
                String rid = JsonPath.read(json, "$.url").toString().replace("/", "");
                String name = JsonPath.read(json, "$.nickname").toString();
                String title = JsonPath.read(json, "$.title").toString();
                String gameName = JsonPath.read(json, "$.gameName").toString();
                String onlineStr = JsonPath.read(json, "$.online").toString();
                int onlineNum = Integer.parseInt(onlineStr);
                int follows = Integer.parseInt(JsonPath.read(json, "$.follows").toString());
                long fight = Long.parseLong(JsonPath.read(json, "$.anchorAttr.hots.fight").toString());//经验值
                indexRec.setLocation(location);
                indexRec.setRid(rid);
                indexRec.setName(name);
                indexRec.setTitle(title);
                indexRec.setJob(Const.ZHANQIINDEXREC);
                indexRec.setUrl(curUrl);
                indexRec.setCategorySec(gameName);
                indexRec.setFollowerNum(follows);
                indexRec.setViewerNum(onlineNum);
                indexRec.setWeightNum(fight);
                detailAnchors.add(indexRec.toString());
            }
        }
    }

    private void executeZhanqiIndex(Page page) {
        Elements scripts = page.getHtml().getDocument().getElementsByTag("script");
        for (Element script : scripts) {
            if (script.toString().contains("window.oPageConfig.aVideos")) {
                String recScript = script.toString();
                String recJson = recScript.substring(recScript.indexOf("[{\""), recScript.indexOf("}];") + 2);
                JSONArray recs = JsonPath.read(recJson, "$.");
                for (int i = 0; i < recs.size(); i++) {
                    String detailUrl = JsonPath.read(recs.get(i).toString(), "$.flashvars.LiveUrl").toString();
                    page.addTargetRequest(new Request(detailUrl).putExtra("location", i + 1));
                }
            }
        }
    }

    private void executePandaV2Json(Page page, String curUrl) {
        String rid = page.getRequest().getExtra("rid").toString();
        IndexRec indexRec = map.get(rid + Const.PANDAINDEXREC);
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
        IndexRec indexRec = map.get(rid + Const.PANDAINDEXREC);
        int follows = JsonPath.read(page.getJson().toString(), "$.data.fans");
        indexRec.setFollowerNum(follows);
    }

    private void executePandaDetail(Page page, String curUrl) {
        Elements scripts = page.getHtml().getDocument().getElementsByTag("script");
        boolean has = false;
        String rid = page.getRequest().getExtra("rid").toString();
        IndexRec indexRec = map.get(rid + Const.PANDAINDEXREC);
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
            map.put(rid + Const.PANDAINDEXREC, indexRec);
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
        zhanqiIndex = "https://www.zhanqi.tv/";
        longzhuIndex = "http://www.longzhu.com/";
        String quanminDf = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
        quanminIndex = "http://www.quanmin.tv/json/page/pc-index/info2.json?_t=" + quanminDf;
        chushouindex = "https://chushou.tv/";
        String hivePaht = Const.COMPETITORDIR + "crawler_indexrec_detail_anchor/" + date;//douyuIndex, huyaIndex, pandaIndex, zhanqiIndex, longzhuIndex, quanminIndex, chushouindex
        Spider.create(new IndexRecProcessor()).thread(1).addUrl(douyuIndex, huyaIndex, pandaIndex, zhanqiIndex, longzhuIndex).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        for (Map.Entry<String, IndexRec> entry : map.entrySet()) {
            detailAnchors.add(entry.getValue().toString());
        }
        CommonTools.writeAndMail(hivePaht, Const.INDEXRECEXIT, detailAnchors);
    }
}
