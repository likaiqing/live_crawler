package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pipeline.QuanminPipeline;
import com.pandatv.tools.IOTools;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;

import java.io.BufferedWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

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
            } else {
                page.setSkip(true);
            }
        } else {
            int curPage = Integer.parseInt(curUrl.substring(curUrl.indexOf("list_") + 5, curUrl.indexOf(".json")));
            if (curPage < pageCount) {
                String addUrl = urlTmp + (curPage + 1) + urlJsonT + format.format(new Date());
                page.addTargetRequest(addUrl);
            } else {
                page.setSkip(true);
            }
        }
        page.putField("json", json);
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "http://www.quanmin.tv/json/play/list.json?_t=";
        String job = args[0];//quanminanchor
        String date = args[1];//20161114
        String hour = args[2];//10
        BufferedWriter bw = IOTools.getBW(Const.FILEDIR + job + "_" + date + "_" + hour + ".csv");
        String dateStr = format.format(new Date());
        Spider.create(new QuanminAnchorProcessor()).addUrl(firUrl + dateStr).addPipeline(new QuanminPipeline(job, bw)).setDownloader(new PandaDownloader()).run();
        IOTools.closeBw(bw);
    }
}
