package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.pipeline.ChushouPipeline;
import com.pandatv.tools.IOTools;
import net.minidev.json.JSONArray;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.selector.Html;
import us.codecraft.webmagic.selector.Json;
import us.codecraft.webmagic.selector.Selectable;

import java.io.BufferedWriter;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class ChushouAnchorProcessor extends PandaProcessor {
    private static String urlTmp = "http://chushou.tv/live/down-v2.htm?&breakpoint=";

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        if (!curUrl.equals("http://chushou.tv/live/list.htm")) {
            String json = page.getJson().toString();
            JSONArray items = JsonPath.read(json, "$.data.items");
            String breakpoint = JsonPath.read(json, "$.data.breakpoint");
            if (items.size() > 0) {
                page.addTargetRequest(urlTmp + breakpoint);
                page.putField("json", page.getJson().toString());
            } else {
                page.setSkip(true);
            }
        } else {
            String breakPoint = page.getHtml().xpath("//div[@class='more']/@data-break").toString();
            page.addTargetRequest(urlTmp + breakPoint);
            Html html = page.getHtml();
            List<String> rids = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='home_live_block']/a/@href").all();
            List<String> titles = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/a/text()").all();
            List<String> names = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='liveDetail']/span[@class='livePlayerName]/text()").all();
            List<String> categories = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='liveDetail']/a[@class='game_Name]/text()").all();
            List<String> popularitiyStrs = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='liveDetail']/span[@class='liveCount]/text()").all();
            page.putField("rids",rids);
            page.putField("names",names);
            page.putField("titles",titles);
            page.putField("categories",categories);
            page.putField("popularitiyStrs",popularitiyStrs);
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "http://chushou.tv/live/list.htm";
        String job = args[0];//zhanqianchor
        String date = args[1];//20161114
        String hour = args[2];//10
        BufferedWriter bw = IOTools.getBW(Const.FILEDIR + job + "_" + date + "_" + hour + ".csv");
        Spider.create(new ChushouAnchorProcessor()).addUrl(firUrl).addPipeline(new ChushouPipeline(job, bw)).run();
    }
}
