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
import us.codecraft.webmagic.selector.Html;

import java.util.*;

/**
 * Created by likaiqing on 2017/8/17.
 */
public class DouyuFullCateProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DouyuFullCateProcessor.class);
    private static final String PCgame = "https://www.douyu.com/directory/category/PCgame";//斗鱼网游竞技
    private static final String djry = "https://www.douyu.com/directory/category/djry";//单机热游
    private static final String syxx = "https://www.douyu.com/directory/category/syxx";//手游休闲
    private static final String yl = "https://www.douyu.com/directory/category/yl";//娱乐天地
    private static final String kjjy = "https://www.douyu.com/directory/category/kjjy";//科技教育
    private static final String voice = "https://www.douyu.com/directory/category/voice";//语音直播
    private static final String znl = "https://www.douyu.com/directory/category/znl";//正能量
    private static final String sep = "\u0001";
    private static Set<String> results = new HashSet<>();//ename cname f_ename f_cname panda_f_ename
    private static Map<String, String> douyuPandaEFullMap = new HashMap();
    private static Map<String, String> pandaFullMap = new HashMap();

    public static void crawler(String[] args) {
        job = args[0];//douyufullcate
        date = args[1];//20161114
        hour = args[2];
        douyuPandaEFullMap.put("PCgame", "jingji");
        douyuPandaEFullMap.put("djry", "zjdj");
        douyuPandaEFullMap.put("syxx", "shouyou");
        douyuPandaEFullMap.put("yl", "yllm");
        douyuPandaEFullMap.put("kjjy", "recorded");
        douyuPandaEFullMap.put("voice", "recorded");
        douyuPandaEFullMap.put("znl", "recorded");

//        pandaFullMap.put("zjdj", "主机单机");
//        pandaFullMap.put("recorded", "大杂烩");
//        pandaFullMap.put("yllm", "娱乐联盟");
//        pandaFullMap.put("shouyou", "手游专区");
//        pandaFullMap.put("jingji", "热门竞技");
//        pandaFullMap.put("wangyou", "网游专区");
        Spider.create(new DouyuFullCateProcessor()).thread(1).addUrl(PCgame, djry, syxx, yl, kjjy, voice).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        logger.info("executeResults resultSetStr.size:" + results.size());
        String dirFile = new StringBuffer(Const.CRAWLER_DATA_DIR).append(date).append("/").append(hour).append("/").append(job).append("_").append(date).append("_").append(hour).append(randomStr).toString();
        CommonTools.write2Local(dirFile, results);
    }

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            Html html = page.getHtml();
            String f_ename = curUrl.substring(curUrl.lastIndexOf("/") + 1);
            executeList(f_ename, html);
        } catch (Exception e) {
            e.printStackTrace();
        }
        page.setSkip(true);
    }

    private void executeList(String f_ename, Html html) {
        String f_cname = html.xpath("//div[@class='real-title js-title pagelive']/text()").get().trim();
        List<String> cateUrls = html.xpath("//ul[@id='live-list-contentbox']/li/a/@href").all();
        List<String> cateCNames = html.xpath("//ul[@id='live-list-contentbox']/li/a/p[@class='title']/text()").all();
        for (int i = 0; i < cateUrls.size(); i++) {
            String cateUrl = cateUrls.get(i);
            String cateEName = cateUrl.substring(cateUrl.lastIndexOf("/") + 1);
            String cateCName = cateCNames.get(i);
            results.add(new StringBuffer(cateEName).append(sep)
                    .append(cateCName).append(sep)
                    .append(f_ename).append(sep)
                    .append(f_cname).append(sep)
                    .append(douyuPandaEFullMap.get(f_ename) == null ? "other" : douyuPandaEFullMap.get(f_ename))
                    .toString());
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }
}
