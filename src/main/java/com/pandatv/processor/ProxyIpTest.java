package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import org.apache.http.HttpHost;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by likaiqing on 2017/12/13.
 */
public class ProxyIpTest extends PandaProcessor {
    //    private static final String url = "http://1212.ip138.com/ic.asp";
//    private static final String url = "http://ip.chinaz.com/getip.aspx";
    private static final String url = "https://www.panda.tv/610956";
    private static Set<String> ips = new HashSet<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static int i = 0;
    private static int total = 2000;

    public static void main(String[] args) {
//        if (args.length == 2 && args[1].matches("\\d+")) {
//            total = Integer.parseInt(args[1]);
//        }
//        Const.GENERATORKEY = "panda";
//        Const.GENERATORPASS = "pandatvpassw0rd";
        Const.GENERATORKEY = "H05972909IM78TAP";
        Const.GENERATORPASS = "36F7B5D8703A39C5";
        Spider.create(new ProxyIpTest()).thread(1).addUrl(url).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
    }

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        try {
//            String[] split = html.xpath("//div[@align='center']/text()").get().split("\\[|\\]");
//            String ip = split[1];
//            String json = page.getJson().get();
//            String ip = JsonPath.read(json, "$.ip").toString();
//            System.out.println(sdf.format(new Date()) + " " + ip);
            System.out.printf(curUrl);
            page.addTargetRequest(url+"?a="+Math.random());
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (i++ < total) {
            page.addTargetRequest(url + "?a=" + Math.random());
        }

    }

    @Override
    public Site getSite() {
//        super.getSite();
        return site.setSleepTime(1000).setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36").setHttpProxy(new HttpHost(Const.ABUYUNPHOST, Const.ABUYUNPORT));
    }
}
