package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.tools.CommonTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

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
        logger.info("process url:{}", url);
        List<String> all = page.getJson().jsonPath("$.data.list").all();
        if (all.size() > 0) {
            page.putField("json", page.getJson().toString());
            String newUrl = this.url + (Integer.parseInt(url.substring(url.lastIndexOf('=') + 1)) + 1);
            page.addTargetRequest(newUrl);
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        job = args[0];//huyaanchor
        date = args[1];
        hour = args[2];
        String hivePaht = Const.HIVEDIR + "panda_anchor_crawler/" + date + hour;
        String firstUrl = "http://www.huya.com/cache.php?m=Live&do=ajaxAllLiveByPage&pageNum=1&page=1";
        Spider.create(new HuyaAnchorProcessor()).thread(1).addUrl(firstUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        CommonTools.writeAndMail(hivePaht, Const.HUYAFINISH, anchors);
    }

    public static void main(String[] args) {
        args = new String[]{"huyaanchor", "20161111", "16"};
        crawler(args);
    }
}
