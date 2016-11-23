package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.pipeline.DouyuAnchorPipeline;
import com.pandatv.tools.IOTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;

import java.io.BufferedWriter;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class DouyuAnchorProccessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DouyuAnchorProccessor.class);
    private static String url = "https://www.douyu.com/directory/all?isAjax=1&page=";
    private static int pageTotal = 0;

    public static void crawler(String[] args) {
        String job = args[0];//douyuanchor
        String date = args[1];
        String hour = args[2];
        BufferedWriter bw = IOTools.getBW(Const.FILEDIR + job + "_" + date + "_" + hour + ".csv");
        String firstUrl = "https://www.douyu.com/directory/all";
//        String url = "https://www.douyu.com/directory/all?isAjax=1&page=1";
//        .setDownloader(new SeleniumDownloader("/Users/likaiqing/Downloads/chromedriver_mac"))
        Spider.create(new DouyuAnchorProccessor()).addUrl(firstUrl).thread(1).addPipeline(new DouyuAnchorPipeline(job, bw)).run();
        IOTools.closeBw(bw);
    }

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().toString();
        List<String> rids = null;
        List<String> names = null;
        List<String> titles = null;
        List<String> popularities = null;
        List<String> categories = null;
        logger.info("process url:{}",curUrl);
        if (curUrl.equals("https://www.douyu.com/directory/all")) {
//            List<String> pages = page.getHtml().xpath("//div[@class='tcd-page-code']/a[@class='shark-pager-item']/text()").all();
//            int endPage = Integer.parseInt(pages.get(pages.size() - 1));
            String js = page.getHtml().getDocument().getElementsByAttributeValue("type", "text/javascript").get(3).toString();
            int endPage = Integer.parseInt(js.substring(js.indexOf("count:") + 8, js.lastIndexOf(',') - 1));
            for (int i = 1; i < endPage; i++) {
                page.addTargetRequest("https://www.douyu.com/directory/all?isAjax=1&page=" + i);
            }
            page.setSkip(true);
        } else {
            rids = page.getHtml().xpath("//body/li/@data-rid").all();
            names = page.getHtml().xpath("//body/li/a/div[@class='mes']/p/span[@class='dy-name ellipsis fl']/text()").all();
            titles = page.getHtml().xpath("//body/li/a/@title").all();
            popularities = page.getHtml().xpath("//body/li/a/div[@class='mes']/p/span[@class='dy-num fr']/text()").all();
            categories = page.getHtml().xpath("//body/li/a/div[@class='mes']/div[@class='mes-tit']/span/text()").all();
            page.putField("rids", rids);
            page.putField("names", names);
            page.putField("titles", titles);
            page.putField("popularities", popularities);
            page.putField("categories", categories);
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void main(String[] args) {
        args = new String[]{"douyuanchor"};
        crawler(args);
    }
}
