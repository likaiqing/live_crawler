package com.pandatv.work;

import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.selenium.SeleniumDownloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.util.List;

/**
 * Created by likaiqing on 2016/11/12.
 */
public class SeleniumProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SeleniumProcessor.class);
    @Override
    public void process(Page page) {
        List<String> pages = page.getHtml().xpath("//div[@class='items items01 item-data clearfix']/ul/li/a/@href").all();
        System.out.println(pages.get(9));
        String curUrl = page.getUrl().toString();
        for (String url : pages){
//            spider.setDownloader(null);
//            spider.run();
            page.addTargetRequest(url);
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void main(String[] args) {
//        System.getProperties().setProperty("webdriver.chrome.driver","src/doc/mac/chromedriver");
        logger.info("seleniumprocessor start");
        String chromeDriverPath = "/home/likaiqing/hive-tool/chromedriver";
//        String chromeDriverPath = "/Users/likaiqing/space/panda/live_crawler/src/doc/mac/chromedriver";
        Spider.create(new SeleniumProcessor()).thread(5).addPipeline(new ConsolePipeline()).addUrl("https://www.douyu.com/directory/all").setDownloader(new SeleniumDownloader(chromeDriverPath)).run();
    }
}
