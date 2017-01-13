package com.pandatv.processor;

import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

/**
 * Created by likaiqing on 2017/1/11.
 */
public class RedirectTest extends PandaProcessor {
    public static void main(String[] args) {
        Spider.create(new RedirectTest()).thread(1).addUrl("https://www.douyu.com/1275878").addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
    }

    @Override
    public void process(Page page) {
        Html html = page.getHtml();
        System.out.println(html);
    }

    @Override
    public Site getSite() {
        return this.site;
    }
}
