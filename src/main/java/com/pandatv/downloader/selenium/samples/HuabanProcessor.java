package com.pandatv.downloader.selenium.samples;

import com.pandatv.downloader.selenium.SeleniumDownloader;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.FilePipeline;
import us.codecraft.webmagic.processor.PageProcessor;

/**
 * Created by likaiqing on 2016/11/16.
 */
public class HuabanProcessor implements PageProcessor {
    private Site site;

    @Override
    public void process(Page page) {
        page.addTargetRequests(page.getHtml().links().regex("http://huaban\\.com/.*").all());
        if (page.getUrl().toString().contains("pins")) {
            page.putField("img", page.getHtml().xpath("//div[@id='pin_img']/a/img/@src").toString());
        } else {
            page.getResultItems().setSkip(true);
        }
    }

    @Override
    public Site getSite() {
        if (null == site) {
            site = Site.me().setDomain("huaban.com").setSleepTime(0);
        }
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new HuabanProcessor()).thread(5)
                .addPipeline(new FilePipeline("/data/webmagic/test/"))
                .setDownloader(new SeleniumDownloader("src/doc/mac/chromedriver"))
                .addUrl("http://huaban.com/")
                .run();
    }
}
