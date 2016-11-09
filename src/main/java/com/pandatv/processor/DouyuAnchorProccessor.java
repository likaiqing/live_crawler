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
    private static int pageTotal = 0;

    public static void crawler(String[] args) {
        String task = args[0];
        String firstUrl = "https://www.douyu.com/directory/all";
        String url = "https://www.douyu.com/directory/all?isAjax=1&page=1";
        Spider.create(new DouyuAnchorProccessor()).addUrl(firstUrl).thread(1).addPipeline(new DouyuAnchorPipeline(task)).run();
    }

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().toString();
        List<String> rids = page.getHtml().xpath("//body/li/@data-rid").all();
        List<String> names = page.getHtml().xpath("//body/li/a/div[@class='mes']/p/span[@class='dy-name ellipsis fl']/text()").all();
        List<String> titles = page.getHtml().xpath("//body/li/a/@title").all();
        List<String> popularities = page.getHtml().xpath("//body/li/a/div[@class='mes']/p/span[@class='dy-num fr']/text()").all();
        List<String> categories = page.getHtml().xpath("//body/li/a/div[@class='mes']/div[@class='mes-tit']/span/text()").all();
        page.putField("rids",rids);
        page.putField("names",names);
        page.putField("titles",titles);
        page.putField("popularities",popularities);
        page.putField("categories",categories);
        if (curUrl.equals("https://www.douyu.com/directory/all")){
            page.addTargetRequest("https://www.douyu.com/directory/all?isAjax=1&page=1");
        }else {
            int pageNum = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf('=') + 1)) + 1;
            if (pageNum<=pageTotal){
                page.addTargetRequest(url + pageNum);
            }
        }


    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void main(String[] args) {
        args=new String[]{"live"};
        crawler(args);
    }
}
