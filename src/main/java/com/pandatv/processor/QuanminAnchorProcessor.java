package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class QuanminAnchorProcessor extends PandaProcessor {
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
    private static String urlTmp = "http://www.quanmin.tv/json/play/list_";
    private static final String urlJsonT = ".json?_t=";
    private static final Logger logger = LoggerFactory.getLogger(QuanminAnchorProcessor.class);
    private static int exCnt;
    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            String json = page.getJson().toString();
            if (StringUtils.isEmpty(json)){
                return;
            }
            int pageCount = JsonPath.read(json, "$.pageCount");
            if (curUrl.startsWith("http://www.quanmin.tv/json/play/list.json?_t=")) {
                if (pageCount > 1) {
                    String addUrl = urlTmp + 2 + urlJsonT + format.format(new Date());
                    page.addTargetRequest(addUrl);
                    addAnchors(anchorObjs, json, curUrl);
                } else {
                    page.setSkip(true);
                }
            } else {
                int curPage = Integer.parseInt(curUrl.substring(curUrl.indexOf("list_") + 5, curUrl.indexOf(".json")));
                if (curPage < pageCount) {
                    String addUrl = urlTmp + (curPage + 1) + urlJsonT + format.format(new Date());
                    page.addTargetRequest(addUrl);
                    addAnchors(anchorObjs, json, curUrl);
                } else {
                    page.setSkip(true);
                }
            }
        }catch (Exception e){
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{},html:{}" + curUrl, page.getHtml());
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail(Const.DOUYUEXIT, "url: " + curUrl);
                System.exit(1);
            }
        }

    }

    private void addAnchors(Set<Anchor> anchors, String json, String curUrl) {
        JSONArray data = JsonPath.read(json, "$.data");
        for (int i = 0; i < data.size(); i++) {
            Anchor anchor = new Anchor();
            String room = data.get(i).toString();
            String rid = JsonPath.read(room, "$.uid");
            String name = JsonPath.read(room, "$.nick");
            String title = JsonPath.read(room, "$.title");
            String category = JsonPath.read(room, "$.category_name");
            String popularityStr = JsonPath.read(room, "$.view");
            int popularityNum = Integer.parseInt(popularityStr);
            anchor.setRid(rid);
            anchor.setName(name);
            anchor.setTitle(title);
            anchor.setCategory(category);
            anchor.setPopularityStr(popularityStr);
            anchor.setPopularityNum(popularityNum);
            anchor.setJob(job);
            anchor.setPlat(Const.QUANMIN);
            anchor.setGame(Const.GAMEALL);
            anchor.setUrl(curUrl);
            anchors.add(anchor);
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "http://www.quanmin.tv/json/play/list.json?_t=";
        job = args[0];//quanminanchor
        date = args[1];//20161114
        hour = args[2];//10
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        String dateStr = format.format(new Date());
        Spider.create(new QuanminAnchorProcessor()).addUrl(firUrl + dateStr).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        for (Anchor anchor : anchorObjs) {
            anchors.add(anchor.toString());
        }
        CommonTools.writeAndMail(hivePaht, Const.QUANMINFINISH, anchors);
    }
}
