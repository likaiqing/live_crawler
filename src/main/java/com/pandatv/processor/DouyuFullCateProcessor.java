package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.tools.HiveJDBCConnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.util.*;

/**
 * Created by likaiqing on 2017/8/17.
 */
public class DouyuFullCateProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DouyuFullCateProcessor.class);
    private static final String gameUrl = "https://www.douyu.com/directory/category/game";
    private static final String ktyxUrl = "https://www.douyu.com/directory/category/ktyx";
    private static final String syxxUrl = "https://www.douyu.com/directory/category/syxx";
    private static final String ylUrl = "https://www.douyu.com/directory/category/yl";
    private static final String kjUrl = "https://www.douyu.com/directory/category/kj";
    private static final String sep = "\u0001";
    private static Set<String> results = new HashSet<>();//ename cname f_ename f_cname panda_f_cname
    private static Map<String, String> douyuPandaEFullMap = new HashMap();
    private static Map<String, String> pandaFullMap = new HashMap();

    public static void crawler(String[] args) {
        job = args[0];//douyufullcate
        date = args[1];//20161114
        hour = args[2];
        Const.GENERATORKEY = "H05972909IM78TAP";
        Const.GENERATORPASS = "36F7B5D8703A39C5";
        douyuPandaEFullMap.put("wykt", "recorded");
        douyuPandaEFullMap.put("znl", "recorded");
        douyuPandaEFullMap.put("game", "jingji");
        douyuPandaEFullMap.put("ktyx", "zjdj");
        douyuPandaEFullMap.put("syxx", "shouyou");
        douyuPandaEFullMap.put("yl", "yllm");
        douyuPandaEFullMap.put("kj", "recorded");

        pandaFullMap.put("zjdj", "主机单机");
        pandaFullMap.put("recorded", "大杂烩");
        pandaFullMap.put("yllm", "娱乐联盟");
        pandaFullMap.put("shouyou", "手游专区");
        pandaFullMap.put("jingji", "热门竞技");
        pandaFullMap.put("wangyou", "网游专区");
        Spider.create(new DouyuFullCateProcessor()).thread(1).addUrl(gameUrl, ktyxUrl, syxxUrl, ylUrl, kjUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        executeResults();
    }

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            Html html = page.getHtml();
            if (curUrl.equals(gameUrl)) {
                executeColumnOther(html);
            }
            String f_ename = curUrl.substring(curUrl.lastIndexOf("/") + 1);
            executeList(f_ename, html);
        } catch (Exception e) {
            e.printStackTrace();
        }
        page.setSkip(true);
    }

    private void executeList(String f_ename, Html html) {
        String f_cname = html.xpath("//div[@class='player-column']//div[@class='real-title js-title pagelive']/text()").get().trim();
        List<String> all = html.xpath("//div[@class='player-column']/div[@id='live-list-content']/ul/li/html()").all();
        for (String a : all) {
            Html aHtml = new Html(a);
            String url = aHtml.xpath("//a/@href").get();
            String title = aHtml.xpath("//p/text()").get();
            results.add(new StringBuffer(url.substring(url.lastIndexOf("/") + 1)).append(sep)
                    .append(title).append(sep)
                    .append(f_ename).append(sep)
                    .append(f_cname).append(sep)
                    .append(douyuPandaEFullMap.get(f_ename) == null ? "other" : douyuPandaEFullMap.get(f_ename))
                    .toString());
        }

    }

    /**
     * 处理热门游戏
     *
     * @param html
     */
    private void executeColumnOther(Html html) {
        String wenYuStr = html.xpath("//div[@class='r-cont column-cont ']/dl/dd[@data-left-item='文娱课堂']/html()").get();
        String zhengNengLiangStr = html.xpath("//div[@class='r-cont column-cont ']/dl/dd[@data-left-item='正能量']/html()").get();
        List<String> wenYuUrls = new Html(wenYuStr).xpath("//ul/li/a/@href").all();
        List<String> zhengNengLiangUrls = new Html(zhengNengLiangStr).xpath("//ul/li/a/@href").all();
        List<String> wenYuTitles = new Html(wenYuStr).xpath("//ul/li/a/@title").all();
        List<String> zhengNengLiangTitles = new Html(zhengNengLiangStr).xpath("//ul/li/a/@title").all();
        for (int i = 0; i < wenYuUrls.size(); i++) {
            String url = wenYuUrls.get(i);
            results.add(new StringBuffer(url.substring(url.lastIndexOf("/") + 1)).append(sep)
                    .append(wenYuTitles.get(i)).append(sep)
                    .append("wykt").append(sep)
                    .append("文娱课堂").append(sep)
                    .append("recorded").toString());
        }
        for (int i = 0; i < zhengNengLiangUrls.size(); i++) {
            String url = zhengNengLiangUrls.get(i);
            results.add(new StringBuffer(url.substring(url.lastIndexOf("/") + 1)).append(sep)
                    .append(zhengNengLiangTitles.get(i)).append(sep)
                    .append("znl").append(sep)
                    .append("正能量").append(sep)
                    .append("recorded").toString());
        }

    }

    @Override
    public Site getSite() {
        return this.site;
    }
}
