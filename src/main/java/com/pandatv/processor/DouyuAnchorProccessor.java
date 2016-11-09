package com.pandatv.processor;

import com.pandatv.common.PandaProcessor;
import com.pandatv.pipeline.DouyuAnchorPipeline;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;

import java.util.List;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class DouyuAnchorProccessor extends PandaProcessor {
    private static String url = "https://www.douyu.com/directory/all?isAjax=1&page=";

    public static void crawler(String[] args) {
        String task = args[0];
        String url = "https://www.douyu.com/directory/all?isAjax=1&page=1";
        Spider.create(new DouyuAnchorProccessor()).addUrl(url).thread(1).addPipeline(new DouyuAnchorPipeline(task)).run();
    }

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().toString();
        List<String> rids = page.getHtml().xpath("//body/li/@data-rid").all();
        List<String> names = page.getHtml().xpath("//body/li").all();

        int pageNum = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf('=') + 1)) + 1;
        page.addTargetRequest(url + pageNum);
    }

    @Override
    public Site getSite() {
        return this.site;
    }
}
