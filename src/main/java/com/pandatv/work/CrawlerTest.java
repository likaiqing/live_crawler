package com.pandatv.work;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.processor.HuyaDetailAnchorProcessor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.HiveJDBCConnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/23.
 */
public class CrawlerTest extends PandaProcessor {
    private static String firstUrl;
    private static final Logger logger = LoggerFactory.getLogger(CrawlerTest.class);
    private static String firstUrl2;
    private static String firstUrl3;
    private static List<String> detailAnchors = new ArrayList<>();


    @Override
    public void process(Page page) {
        System.out.println("url"+page.getUrl());
        if (page.getUrl().get().equals("https://www.douyu.com/")){
            for (int i=0;i<10;i++){
                page.addTargetRequest("https://www.douyu.com/?"+(Math.random()*100000));
            }
        }
        if (page.getUrl().get().equals("http://1212.ip138.com/ic.asp?")){
            for (int i=0;i<10;i++){
                page.addTargetRequest("http://1212.ip138.com/ic.asp?1"+(Math.random()*100000));
            }
        }
        if (page.getUrl().get().startsWith("http://1212.ip138.com/ic.asp?1")){
            Html html = page.getHtml();
            String center = html.xpath("//center/text()").get();
            String ip = center.substring(center.indexOf("[") + 1, center.indexOf("]"));
            detailAnchors.add(ip);
            System.out.println("curUrl"+page.getUrl());
        }
        if (page.getUrl().get().startsWith("")){
            page.addTargetRequest(firstUrl3+"?"+Math.random()*1000);
        }
    }

    @Override
    public Site getSite() {
        return CommonTools.getAbuyunSite(site);
//        return this.site;
    }

    public static void main(String[] args) {
        firstUrl = "https://www.douyu.com/";
        firstUrl2 = "http://1212.ip138.com/ic.asp?";
        firstUrl3 = "http://www.panda.tv/610956";
        HiveJDBCConnect hive = new HiveJDBCConnect();
        String hivePaht = Const.HIVEDIR + "panda_detail_anchor_crawler/2016112413" ;
        long s = System.currentTimeMillis();
        Spider.create(new CrawlerTest()).thread(1).addUrl(firstUrl3).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long e = System.currentTimeMillis();//.setDownloader(new PandaDownloader())
        hive.write2(hivePaht, detailAnchors);
        System.out.println("e-s:"+(e-s));
    }
}
