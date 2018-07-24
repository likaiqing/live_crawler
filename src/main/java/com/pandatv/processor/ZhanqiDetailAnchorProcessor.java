package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.apache.http.HttpHost;
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
    private static final String urlTmp = "https://www.zhanqi.tv/api/static/v2.1/live/list/20/";
    private static final String jsonStr = ".json";
    //    private static final int cntPerPage = 30;
    private static final String domain = "https://www.zhanqi.tv";
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static final Logger logger = LoggerFactory.getLogger(ZhanqiDetailAnchorProcessor.class);
    private static int exCnt;

    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            if (curUrl.startsWith(urlTmp)) {
                int curPage = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf("/") + 1, curUrl.lastIndexOf(".json")));
                String json = page.getJson().toString();
                int cnt = JsonPath.read(json, "$.data.cnt");
                if (curPage * 20 < cnt) {
                    page.addTargetRequest(urlTmp + (curPage + 1) + jsonStr);
                }
                JSONArray rooms = JsonPath.read(json, "$.data.rooms");
                for (int i = 0; i < rooms.size(); i++) {
                    String room = rooms.get(i).toString();
                    String url = JsonPath.read(room, "$.url");
                    Request request = new Request(domain + url).putExtra("rid", url.replace("/", ""));
                    page.addTargetRequest(request);
                }
            } else {
                DetailAnchor detailAnchor = new DetailAnchor();
                List<String> allScript = page.getHtml().xpath("//script").all();
                for (String script : allScript) {
                    if (script.contains("window.oPageConfig.oRoom")) {
                        String rid = page.getRequest().getExtra("rid").toString();
                        int start = script.indexOf("nickname") + 11;
                        String name = script.substring(start, script.indexOf("\"", start));
                        start = script.indexOf("title") + 8;
                        String title = script.substring(start, script.indexOf("\"", start));
                        start = script.indexOf("gameName") + 11;
                        String gameName = script.substring(start, script.indexOf("\"", start));
                        start = script.indexOf("online") + 9;
                        String onlineStr = script.substring(start, script.indexOf("\"", start));
                        int onlineNum = Integer.parseInt(onlineStr);
                        String liveTime = "";
                        String lastStartTime = "";
                        int follows = 0;
                        long fight = 0;
                        try {
                            start = script.indexOf("follows") + 10;
                            follows = Integer.parseInt(script.substring(start, script.indexOf("\"", start)));
                            start = script.indexOf("fight") + 9;
                            fight = Long.parseLong(script.substring(start, script.indexOf("\"", start)));//经验值
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                            logger.error("NumberFormatException url:{})", curUrl);
                        }
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
                        resultSetStr.add(detailAnchor.toString());
                    }
                }
            }
            page.setSkip(true);
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{}", curUrl);
            e.printStackTrace();
            if (++exCnt % 100 == 0) {
                MailTools.sendAlarmmail("zhanqidetailanchor 异常请求个数过多", "url: " + failedUrl.toString());
//                System.exit(1);
            }
        }
    }

    private String getLastStartTime(long broadcastBegin) {
        return format.format(broadcastBegin);
    }


    @Override
    public Site getSite() {
        super.getSite();
        site.setHttpProxy(new HttpHost(Const.ABUYUNPHOST, Const.ABUYUNPORT));
        return this.site.setSleepTime(400);
    }

    public static void crawler(String[] args) {
        String firUrl = "https://www.zhanqi.tv/api/static/v2.1/live/list/20/1.json";
        job = args[0];//zhanqidetailanchor
        date = args[1];//20161114
        hour = args[2];//10
//        Const.GENERATORKEY = "H05972909IM78TAP";
//        Const.GENERATORPASS = "36F7B5D8703A39C5";
        Const.GENERATORKEY = "H7ABSOS1FI3M9I4P";
        Const.GENERATORPASS = "97CCB7E9284ACAF0";
        initParam(args);
        String hivePaht = Const.COMPETITORDIR + "crawler_detail_anchor/" + date;
        //钩子
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutDownHook()));

        long start = System.currentTimeMillis();
        Spider.create(new ZhanqiDetailAnchorProcessor()).thread(1).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000 + 1;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
//        for (DetailAnchor detailAnchor : detailAnchorObjs) {
//            detailAnchors.add(detailAnchor.toString());
//        }
//        CommonTools.writeAndMail(hivePaht, Const.ZHANQIFINISHDETAIL, detailAnchors);

        executeResults();
    }
}
