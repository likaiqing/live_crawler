package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.HttpUtil;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class ZhanqiAnchorProcessor extends PandaProcessor {
    private static final String urlTmp = "https://www.zhanqi.tv/api/static/v2.1/live/list/30/";
    private static final String jsonStr = ".json";
    //    private static final int cntPerPage = 30;
    private static final Logger logger = LoggerFactory.getLogger(ZhanqiAnchorProcessor.class);
    private static int exCnt;

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            int curPage = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf("/") + 1, curUrl.lastIndexOf(".json")));
            String json = page.getJson().toString();
            int cnt = JsonPath.read(json, "$.data.cnt");
            if (curPage * 30 < cnt) {
                page.addTargetRequest(urlTmp + (curPage + 1) + jsonStr);
            }
            JSONArray rooms = JsonPath.read(json, "$.data.rooms");
            List<String> results = new ArrayList<>();
            for (int i = 0; i < rooms.size(); i++) {
                String room = rooms.get(i).toString();
                String rid = JsonPath.read(room, "$.url");
                String name = JsonPath.read(room, "$.nickname");
                String title = JsonPath.read(room, "$.title");
                String category = JsonPath.read(room, "$.gameName");
                String popularityStr = JsonPath.read(room, "$.online");
                int popularityNum = Integer.parseInt(popularityStr);
                HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
                        .append("&par_d=").append(date)
                        .append("&rid=").append(rid)
                        .append("&nm=").append(CommonTools.getFormatStr(name))
                        .append("&tt=").append(CommonTools.getFormatStr(title))
                        .append("&cate=").append(category)
                        .append("&pop_s=").append(popularityStr)
                        .append("&pop_n=").append(popularityNum)
                        .append("&task=").append(job)
                        .append("&plat=").append(Const.ZHANQI)
                        .append("&url_c=").append(Const.GAMEALL)
                        .append("&c_time=").append(createTimeFormat.format(new Date()))
                        .append("&url=").append(curUrl)
                        .append("&t_ran=").append(PandaProcessor.getRandomStr()).toString());
//                Anchor anchor = new Anchor();
//                anchor.setRid(rid.replace("/", ""));
//                anchor.setName(name);
//                anchor.setTitle(title);
//                anchor.setCategory(category);
//                anchor.setPopularityStr(popularityStr);
//                anchor.setPopularityNum(popularityNum);
//                anchor.setJob(job);
//                anchor.setPlat(Const.ZHANQI);
//                anchor.setGame(Const.GAMEALL);
//                anchor.setUrl(curUrl);
//                anchorObjs.add(anchor);
            }
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{}", curUrl);
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail(Const.DOUYUEXIT, "url: " + curUrl);
                System.exit(1);
            }
        }

    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "https://www.zhanqi.tv/api/static/v2.1/live/list/30/1.json";
        job = args[0];//zhanqianchor
        date = args[1];//20161114
        hour = args[2];//10
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        Spider.create(new ZhanqiAnchorProcessor()).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
//        for (Anchor anchor : anchorObjs) {
//            anchors.add(anchor.toString());
//        }
//        CommonTools.writeAndMail(hivePaht, Const.ZHANQIFINISH, anchors);
    }
}
