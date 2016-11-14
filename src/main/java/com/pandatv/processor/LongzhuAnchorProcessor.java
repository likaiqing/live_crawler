package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.pipeline.LongzhuPipeLine;
import com.pandatv.tools.IOTools;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;

import java.io.BufferedWriter;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class LongzhuAnchorProcessor extends PandaProcessor {
    private static String urlTmp = "http://api.plu.cn/tga/streams?max-results=18&sort-by=views&filter=0&game=0&callback=_callbacks_._36bxu1&start-index=";
    private static int pageCount = 18;
    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        String body = page.getHtml().xpath("//body/text()").toString();
        String json = body.substring(body.indexOf('(')+1,body.lastIndexOf(')'));
        Integer total = JsonPath.read(json, "$.data.totalItems");
        int index = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf('=')+1));
        if (index<total){
            page.addTargetRequest(urlTmp+(index+pageCount));
        }
        page.putField("json",json);
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "http://api.plu.cn/tga/streams?max-results=18&sort-by=views&filter=0&game=0&callback=_callbacks_._36bxu1&start-index=0";
        String job = args[0];//longzhuanchor
        String date = args[1];//20161114
        String hour = args[2];//10
        BufferedWriter bw = IOTools.getBW(Const.FILEDIR + job + "_" + date + "_" + hour + ".csv");
        Spider.create(new LongzhuAnchorProcessor()).addUrl(firUrl).addPipeline(new LongzhuPipeLine(job,bw)).run();
    }
}
