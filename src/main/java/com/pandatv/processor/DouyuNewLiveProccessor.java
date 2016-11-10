package com.pandatv.processor;

import com.pandatv.common.PandaProcessor;
import com.pandatv.pipeline.DouyuNewlivePipeline;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class DouyuNewLiveProccessor extends PandaProcessor {
    private static int index = 30;
    private static String url = "https://www.douyu.com/member/recommlist/getfreshlistajax?bzdata=0&clickNum=";

    @Override
    public void process(Page page) {
        String json = page.getJson().toString();
        String curUrl = page.getUrl().toString();
        int curNum = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf('=')+1));
        if (curNum<index){
            page.putField("json",json);
            page.addTargetRequest(url+(curUrl+1));
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String job = args[0];
        Spider.create(new DouyuNewLiveProccessor()).addUrl(url+"1").thread(1).addPipeline(new DouyuNewlivePipeline(job)).run();
    }
}
