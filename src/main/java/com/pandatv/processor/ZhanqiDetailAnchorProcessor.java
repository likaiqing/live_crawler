package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class ZhanqiDetailAnchorProcessor extends PandaProcessor {
    private static final String urlTmp = "https://www.zhanqi.tv/api/static/v2.1/live/list/30/";
    private static final String jsonStr = ".json";
    //    private static final int cntPerPage = 30;
    private static final String domain = "https://www.zhanqi.tv";
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static final Logger logger = LoggerFactory.getLogger(ZhanqiDetailAnchorProcessor.class);
    private static int exCnt;
    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            if (curUrl.startsWith(urlTmp)) {
                int curPage = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf("/") + 1, curUrl.lastIndexOf(".json")));
                String json = page.getJson().toString();
                int cnt = JsonPath.read(json, "$.data.cnt");
                if (curPage * 30 < cnt) {
                    page.addTargetRequest(urlTmp + (curPage + 1) + jsonStr);
                }
                JSONArray rooms = JsonPath.read(json, "$.data.rooms");
                for (int i = 0; i < rooms.size(); i++) {
                    String room = rooms.get(i).toString();
                    String url = JsonPath.read(room, "$.url");
                    page.addTargetRequest(new Request(domain + url).putExtra("rid", url.replace("/", "")));
                }
            } else {
                DetailAnchor detailAnchor = new DetailAnchor();
                List<String> allScript = page.getHtml().xpath("//script").all();
                for (String script : allScript) {
                    if (script.contains("window.oPageConfig.oRoom")) {
                        String json = script.substring(script.indexOf("{"), script.lastIndexOf("}") + 1);
                        String rid = page.getRequest().getExtra("rid").toString();
                        String name = JsonPath.read(json, "$.nickname").toString();
                        String title = JsonPath.read(json, "$.title").toString();
                        String gameName = JsonPath.read(json, "$.gameName").toString();
                        String onlineStr = JsonPath.read(json, "$.online").toString();
                        int onlineNum = Integer.parseInt(onlineStr);
                        String liveTime = JsonPath.read(json, "$.liveTime").toString();
                        String lastStartTime = getLastStartTime(Long.parseLong(liveTime) * 1000);
                        int follows = Integer.parseInt(JsonPath.read(json, "$.follows").toString());
                        long fight = Long.parseLong(JsonPath.read(json, "$.anchorAttr.hots.fight").toString());//经验值
                        detailAnchor.setRid(rid);
                        detailAnchor.setName(name);
                        detailAnchor.setTitle(title);
                        detailAnchor.setJob(job);
                        detailAnchor.setUrl(curUrl);
                        detailAnchor.setLastStartTime(lastStartTime);
                        detailAnchor.setCategorySec(gameName);
                        detailAnchor.setFollowerNum(follows);
                        detailAnchor.setViewerNum(onlineNum);
                        detailAnchor.setWeightNum(fight);
                        detailAnchorObjs.add(detailAnchor);
                    }
                }
            }
            page.setSkip(true);
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{},html:{}" + curUrl, page.getHtml());
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail(Const.DOUYUEXIT, "url: " + curUrl);
                System.exit(1);
            }
        }
    }

    private String getLastStartTime(long broadcastBegin) {
        return format.format(broadcastBegin);
    }


    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "https://www.zhanqi.tv/api/static/v2.1/live/list/30/1.json";
        job = args[0];//zhanqidetailanchor
        date = args[1];//20161114
        hour = args[2];//10
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_detail_anchor/" + date;
        Spider.create(new ZhanqiDetailAnchorProcessor()).thread(5).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        for (DetailAnchor detailAnchor : detailAnchorObjs) {
            detailAnchors.add(detailAnchor.toString());
        }
        CommonTools.writeAndMail(hivePaht, Const.ZHANQIFINISHDETAIL, detailAnchors);
    }
}
