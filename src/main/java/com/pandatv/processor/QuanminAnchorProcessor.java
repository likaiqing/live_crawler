package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.CommonTools;
import net.minidev.json.JSONArray;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class QuanminAnchorProcessor extends PandaProcessor {
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
    private static String urlTmp = "http://www.quanmin.tv/json/play/list_";
    private static final String urlJsonT = ".json?_t=";

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        String json = page.getJson().toString();
        int pageCount = JsonPath.read(json, "$.pageCount");
        if (curUrl.startsWith("http://www.quanmin.tv/json/play/list.json?_t=")) {
            if (pageCount > 1) {
                String addUrl = urlTmp + 2 + urlJsonT + format.format(new Date());
                page.addTargetRequest(addUrl);
                addAnchors(anchors, json, curUrl);
            } else {
                page.setSkip(true);
            }
        } else {
            int curPage = Integer.parseInt(curUrl.substring(curUrl.indexOf("list_") + 5, curUrl.indexOf(".json")));
            if (curPage < pageCount) {
                String addUrl = urlTmp + (curPage + 1) + urlJsonT + format.format(new Date());
                page.addTargetRequest(addUrl);
                addAnchors(anchors, json, curUrl);
            } else {
                page.setSkip(true);
            }
        }
    }

    private void addAnchors(List<String> anchors, String json, String curUrl) {
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
            String result = anchor.toString();
            anchors.add(result);
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
        String hivePaht = Const.HIVEDIR + "panda_anchor_crawler/" + date + hour;
        String dateStr = format.format(new Date());
        Spider.create(new QuanminAnchorProcessor()).addUrl(firUrl + dateStr).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        CommonTools.writeAndMail(hivePaht, Const.QUANMINFINISHDETAIL, anchors);
    }
}
