package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.pipeline.ZhanqiPipeline;
import com.pandatv.tools.IOTools;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;

import java.io.BufferedWriter;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class ZhanqiAnchorProcessor extends PandaProcessor {
    private static final String urlTmp = "https://www.zhanqi.tv/api/static/v2.1/live/list/30/";
    private static final String jsonStr = ".json";
//    private static final int cntPerPage = 30;

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        int curPage = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf("/") + 1, curUrl.lastIndexOf(".json")));
        String json = page.getJson().toString();
        int cnt = JsonPath.read(json, "$.data.cnt");
        if (curPage * 30 < cnt) {
            page.addTargetRequest(urlTmp + (curPage + 1) + jsonStr);
        }
        page.putField("json",json);
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "https://www.zhanqi.tv/api/static/v2.1/live/list/30/1.json";
        String job = args[0];//zhanqianchor
        String date = args[1];//20161114
        String hour = args[2];//10
        BufferedWriter bw = IOTools.getBW(Const.FILEDIR + job + "_" + date + "_" + hour + ".csv");
        Spider.create(new ZhanqiAnchorProcessor()).addUrl(firUrl).addPipeline(new ZhanqiPipeline(job, bw)).run();
        IOTools.closeBw(bw);
    }
}
