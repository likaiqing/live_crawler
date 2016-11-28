package com.pandatv.work;

import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.selenium.SeleniumDownloader;
import com.pandatv.tools.CommonTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Selectable;

import java.util.List;

/**
 * Created by likaiqing on 2016/11/12.
 */
public class SeleniumProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SeleniumProcessor.class);

    @Override
    public void process(Page page) {
        String url = page.getUrl().get();
//        List<String> pages = page.getHtml().xpath("//div[@class='items items01 item-data clearfix']/ul/li/a/@href").all();
//        System.out.println(pages.get(9));
//        String curUrl = page.getUrl().toString();
//        if (curUrl.equals("https://www.douyu.com/directory/all")) {
//            for (String url : pages) {
//                page.addTargetRequest(url);
//            }
//        }
        System.out.println(page.getHtml().get());
    }

    @Override
    public Site getSite() {
        return CommonTools.getAbuyunSite(site).setSleepTime(1000);
//        return this.site;
    }

    public static void crawler(String[] args) {
//        System.getProperties().setProperty("webdriver.chrome.driver","src/doc/mac/chromedriver");
        logger.info("seleniumprocessor start");
//        String chromeDriverPath = "/home/likaiqing/hive-tool/chromedriver";
        String firUrl = "https://www.douyu.com/directory/all";
        String secUrl = "http://1212.ip138.com/ic.asp";
        String chromeDriverPath = "/data/tmp/crawler_driver/chromedriver";
        Spider.create(new SeleniumProcessor()).thread(1).addPipeline(new ConsolePipeline()).addUrl(firUrl,secUrl).setDownloader(new SeleniumDownloader(chromeDriverPath)).run();
    }

    public static void main(String[] args) {
        crawler(args);
    }
}
