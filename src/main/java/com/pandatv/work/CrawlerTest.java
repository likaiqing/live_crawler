package com.pandatv.work;

import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.tools.CommonTools;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

/**
 * Created by likaiqing on 2016/11/23.
 */
public class CrawlerTest extends PandaProcessor {
    private static String firstUrl;
    private static String firstUrl2;


    @Override
    public void process(Page page) {
        if (page.getUrl().get().equals("http://1212.ip138.com/ic.asp")){
            for (int i=0;i<300;i++){
                page.addTargetRequest("http://1212.ip138.com/ic.asp?"+(Math.random()*100000));
            }
        }
        Html html = page.getHtml();
        String center = html.xpath("//center/text()").get();
        String ip = center.substring(center.indexOf("[") + 1, center.indexOf("]"));
        System.out.println(ip);
    }

    @Override
    public Site getSite() {
        return CommonTools.getAbuyunSite(site);
//        return this.site;
    }

    public static void main(String[] args) {
        firstUrl = "http://1212.ip138.com/ic.asp";
        firstUrl2 = "http://1212.ip138.com/ic.asp?";
        long s = System.currentTimeMillis();
        Spider.create(new CrawlerTest()).thread(8).addUrl(firstUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long e = System.currentTimeMillis();//.setDownloader(new PandaDownloader())
        System.out.println("e-s:"+(e-s));
    }
}
