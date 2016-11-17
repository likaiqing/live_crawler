package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.selenium.SeleniumDownloader;
import com.pandatv.pipeline.DouyuAnchorDetailPipeline;
import com.pandatv.tools.IOTools;
import org.apache.commons.lang.StringUtils;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.selector.Html;

import java.io.BufferedWriter;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/15.
 */
public class DouyuDetailAnchorProcessor extends PandaProcessor {
    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().toString();
        Html html = page.getHtml();
        if (curUrl.equals("https://www.douyu.com/directory/all")) {
            String js = page.getHtml().getDocument().getElementsByAttributeValue("type", "text/javascript").get(3).toString();
            int endPage = Integer.parseInt(js.substring(js.indexOf("count:") + 8, js.lastIndexOf(',') - 1));
            for (int i = 1; i < endPage; i++) {
                Request request = new Request("https://www.douyu.com/directory/all?isAjax=1&page=" + i).setPriority(1);
                page.addTargetRequest(request);
                page.setSkip(true);
            }
        } else if (curUrl.startsWith("https://www.douyu.com/directory/all?isAjax=1&page=")) {
            List<String> detailUrls = page.getHtml().xpath("//body/li/a/@href").all();
            for (String url : detailUrls) {
                Request request = new Request(url).setPriority(3);
                page.addTargetRequest(request);
                page.setSkip(true);
            }
        } else {
            String name = html.xpath("//div[@class='relate-text fl']/div[@class='acinfo-fs-con  clearfix']/ul[@class='r-else clearfix']/li[0]/div[@class='zb-name-con']/a/text()").get();//比较多的
            String title = null;
            String viewerStr = null;
            String roomTag = null;
            String weight = null;
            String followerStr = null;
            String notice = null;
            String notice1 = null;
            if (!StringUtils.isEmpty(name)) {
                title = html.xpath("//div[@class='headline clearfix']/h1/text()").get();
                viewerStr = html.xpath("//div[@class='relate-text fl']/div[@class='acinfo-fs-con  clearfix']/ul[@class='r-else clearfix']/li[1]/div[@class='num-box']/div[@class='num-v-con']/a/text()").get();
                roomTag = html.xpath("//div[@class='tag-fs-con clearfix']/dl/dd/a/text()").get();


            } else {//有可能是转播的qq直播
                html.xpath("//div[@class='relate-text']/ul[@class='r-else clearfix']/li[0]/i[@class='zb-name']/text()").get();
                title = html.xpath("//div[@class='relate-text']/ul[@class='headline clearfix']/h1/text()").get();
                viewerStr = html.xpath("//div[@class='relate-text']/ul[@class='r-else clearfix']/li[1]/span[@class='num-box']/span[@class='num-v']/text()").get();
                roomTag = html.xpath("//div[@class='relate-text']/ul[@class='r-else clearfix']/li[2]/a/text()").get();
                weight = html.xpath("//div[@class='relate-text']/ul[@class='r-else clearfix']/li[3]/span[@class='weight-box']/span[@class='weight-v']/text()").get();
                followerStr = html.xpath("//div[@class='btn-group fr']/div[@class='focus-box']/p/span/text()").get();
                notice = html.xpath("//div[@class='column o-notice']/div[@class='column-cont']/p/span/text()").get();
                notice1 = html.xpath("//div[@id='js-notice']/p/span/text()").get();
            }

            page.putField("name", name);
        }
        page.putField("curUrl", curUrl);
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String job = args[0];//douyuanchordetail
        String date = args[1];
        String hour = args[2];
        BufferedWriter bw = IOTools.getBW(Const.FILEDIR + job + "_" + date + "_" + hour + ".csv");
        String firstUrl = "https://www.douyu.com/directory/all";
        Spider.create(new DouyuDetailAnchorProcessor()).thread(2).addUrl(firstUrl).setDownloader(new SeleniumDownloader(Const.CHROMEDRIVER)).addPipeline(new DouyuAnchorDetailPipeline(job, bw)).run();
        IOTools.closeBw(bw);
    }
}
