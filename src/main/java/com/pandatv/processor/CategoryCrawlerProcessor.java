package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.tools.CommonTools;
import net.minidev.json.JSONArray;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by likaiqing on 2016/12/13.
 */
public class CategoryCrawlerProcessor extends PandaProcessor {
    private static Set<String> categories = new HashSet<>();

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        Html html = page.getHtml();
        if (curUrl.equals("https://www.douyu.com/directory")) {
            List<String> urls = html.xpath("//ul[@id='live-list-contentbox']/li/a/@href").all();
            List<String> names = html.xpath("//ul[@id='live-list-contentbox']/li/a/p/text()").all();
            add2Categories(curUrl, urls, names, PlatIdEnum.DOUYU);
        } else if (curUrl.equals("http://www.huya.com/g")) {
            List<String> urls = html.xpath("//ul[@id='js-game-list']/li/a/@href").all();
            List<String> names = html.xpath("//ul[@id='js-game-list']/li/a/p/text()").all();
            add2Categories(curUrl, urls, names, PlatIdEnum.HUYA);
        } else if (curUrl.equals("https://www.zhanqi.tv/games")) {
            List<String> urls = html.xpath("//ul[@id='game-list-panel']/li/a/@href").all();
            List<String> names = html.xpath("//ul[@id='game-list-panel']/li/a/p/text()").all();
            add2Categories(curUrl, urls, names, PlatIdEnum.ZHANQI);
        } else if (curUrl.startsWith("http://www.quanmin.tv")) {
            JSONArray jsonArray = JsonPath.read(page.getJson().get(), "$");
            for (Object obj : jsonArray) {
                String eName = JsonPath.read(obj, "$.slug");
                String cName = JsonPath.read(obj, "$.name");
                String url = "http://www.quanmin.tv/game/" + eName;
                StringBuffer sb = new StringBuffer(PlatIdEnum.QUANMIN.platId);
                sb.append(Const.SEP).append(PlatIdEnum.QUANMIN.paltName).append(Const.SEP).append(eName).append(Const.SEP).append(cName).append(Const.SEP).append(url).append(Const.SEP).append(curUrl).append(Const.SEP).append(getRandomStr());
                categories.add(sb.toString());
            }
        } else if (curUrl.equals("http://longzhu.com/games/?from=rmallgames")) {
            List<String> urls = html.xpath("//div[@class='list-con']/div[@class='list-item']/h2/a/@href").all();
            List<String> names = html.xpath("//div[@class='list-con']/div[@class='list-item']/h2/a/text()").all();
            add2Categories(curUrl, urls, names, PlatIdEnum.LONGZHU);
        } else if (curUrl.equals("http://chushou.tv/gamezone.htm")) {
            List<String> urls = html.xpath("//div[@class='gamezoneBlock']/div[@class='per_gamezone_block']/div[@class='per_gamezone_content']/a/@href").all();
            List<String> names = html.xpath("//div[@class='gamezoneBlock']/div[@class='per_gamezone_block']/div[@class='per_gamezone_content']/a/span/text()").all();
            add2Categories(curUrl, urls, names, PlatIdEnum.CHUSHOU);
        } else if (curUrl.equals("http://www.panda.tv/cate")) {
            List<String> urls = html.xpath("//ul[@class='sort-menu video-list clearfix']/li/a/@href").all();
            List<String> names = html.xpath("//ul[@class='sort-menu video-list clearfix']/li/a/div[@class='cate-title']/text()").all();
            add2Categories(curUrl, urls, names, PlatIdEnum.PANDA);
        }
    }

    private void add2Categories(String curUrl, List<String> urls, List<String> names, PlatIdEnum platIdEnum) {
        for (int i = 0; i < urls.size(); i++) {
            StringBuffer sb = new StringBuffer();
            String url = urls.get(i);
            String name = names.get(i);
            sb.append(platIdEnum.platId).append(Const.SEP).append(platIdEnum.paltName).append(Const.SEP).append(url.substring(url.lastIndexOf("/") + 1)).append(Const.SEP).append(name).append(Const.SEP).append(url).append(Const.SEP).append(curUrl).append(Const.SEP).append(getRandomStr());
            categories.add(sb.toString());
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        job = args[0];//categorycrawler
        date = args[1];//20161114
        hour = args[2];//
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_category/" + date;
        Spider.create(new CategoryCrawlerProcessor()).addUrl("https://www.douyu.com/directory", "http://www.huya.com/g", "https://www.zhanqi.tv/games", "http://www.quanmin.tv/json/categories/list.json?_t=" + new SimpleDateFormat("yyyyMMddHHmm").format(new Date()), "http://longzhu.com/games/?from=rmallgames", "http://chushou.tv/gamezone.htm", "http://www.panda.tv/cate").addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        CommonTools.writeAndMail(hivePaht, Const.CATEGORYFINISH, categories);
    }

    public static enum PlatIdEnum {
        PANDA(1, "panda"),
        DOUYU(2, "douyu"),
        HUYA(3, "huya"),
        ZHANQI(4, "zhanqi"),
        QUANMIN(5, "quanmin"),
        LONGZHU(6, "longzhu"),
        CHUSHOU(7, "chushou");

        private int platId;
        private String paltName;

        private PlatIdEnum(int platId, String paltName) {
            this.platId = platId;
            this.paltName = paltName;
        }
    }
}
