package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.tools.CommonTools;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by likaiqing on 2017/12/13.
 */
public class ProxyIpTest extends PandaProcessor {
//    private static final String url = "http://1212.ip138.com/ic.asp";
    private static final String url = "http://www.ip138.com/";
    private static Set<String> ips = new HashSet<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private static int i = 0;
    private static int total = 2000;

    public static void main(String[] args) {
        if (args.length == 2 && args[1].matches("\\d+")) {
            total = Integer.parseInt(args[1]);
        }
        Const.GENERATORKEY = "panda";
        Const.GENERATORPASS = "pandatvpassw0rd";
        Spider.create(new ProxyIpTest()).thread(thread).addUrl(url).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
    }

    @Override
    public void process(Page page) {
        Html html = page.getHtml();
        String curUrl = page.getUrl().get();
        try {
//            String[] split = html.xpath("//div[@align='center']/text()").get().split("\\[|\\]");
//            String ip = split[1];
            String ip = html.xpath("//div[@class='well']/p/code/text()").get();
            ips.add(sdf.format(new Date()) + "--" + ip);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (i++ < total) {
            page.addTargetRequest(url + "?a=" + Math.random());
        }

    }

    @Override
    public Site getSite() {
        super.getSite();
        return site.setSleepTime(2000).setCharset("gb2312");
    }
}
