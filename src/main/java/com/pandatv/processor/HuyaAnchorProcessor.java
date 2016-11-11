package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.pipeline.HuyaAnchorPipeline;
import com.pandatv.tools.IOTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;

import java.io.BufferedWriter;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/11.
 */
public class HuyaAnchorProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HuyaAnchorProcessor.class);
    private static String url = "http://www.huya.com/cache.php?m=Live&do=ajaxAllLiveByPage&pageNum=1&page=";
    @Override
    public void process(Page page) {
        String url = page.getUrl().toString();
        logger.info("process url:{}",url);
        List<String> all = page.getJson().jsonPath("$.data.list").all();
        if (all.size()>0){
            page.putField("json",page.getJson().toString());
            String newUrl = this.url+(Integer.parseInt(url.substring(url.lastIndexOf('=')+1))+1);
            page.addTargetRequest(newUrl);
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String job = args[0];//huyaanchor
        String date = args[1];
        String hour = args[2];
        BufferedWriter bw = IOTools.getBW(Const.FILEDIR + job + "_" + date + "_" + hour + ".csv");
        String firstUrl = "http://www.huya.com/cache.php?m=Live&do=ajaxAllLiveByPage&pageNum=1&page=1";
        Spider.create(new HuyaAnchorProcessor()).thread(1).addUrl(firstUrl).addPipeline(new HuyaAnchorPipeline(job,bw)).run();
        IOTools.closeBw(bw);
    }

    public static void main(String[] args) {
        args = new String[]{"huyaanchor","20161111","16"};
        crawler(args);
    }
}
