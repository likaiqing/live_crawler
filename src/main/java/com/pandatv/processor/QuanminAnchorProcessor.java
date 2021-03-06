package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.MailTools;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class QuanminAnchorProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(QuanminAnchorProcessor.class);
    private static int exCnt;
    private static String firUrl = "https://www.quanmin.tv/game/all";

    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            if (!curUrl.equals(firUrl)) {
                parsePage(page, curUrl);
            } else {
                List<String> urls = page.getHtml().xpath("//div[@class='list_w-videos_paging']/a/@href").all();
                int maxPage = urls.stream().filter(url -> url.contains("all?p=")).map(url -> Integer.parseInt(url.substring(url.lastIndexOf("=") + 1).trim())).reduce((a, b) -> a > b ? a : b).get();
                if (maxPage > 1) {
                    IntStream.range(2, maxPage).forEach(p -> page.addTargetRequest(firUrl + "?p=" + p));
                }
                parsePage(page, curUrl);
            }

        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{}" + curUrl);
            e.printStackTrace();
            if (++exCnt % 5 == 0) {
                MailTools.sendAlarmmail("quanminanchor 异常请求个数过多", "url: " + curUrl);
//                System.exit(1);
            }
        }

    }

    private void parsePage(Page page, String curUrl) {
        List<String> all = page.getHtml().xpath("//div[@class='list_w-videos'][2]/ul[@class='list_w-videos_video-list']/li/html()").all();
        for (int i = 0; i < all.size(); i++) {
            Anchor anchor = new Anchor();
            Html html = new Html(all.get(i));
            String url = html.xpath("//a[@class='common_w-card_href']/@href").get();
            String rid = url.substring(url.lastIndexOf("/") + 1);
            String title = html.xpath("//div[@class='common_w-card_bottom']/div/p/text()").get();
            String name = html.xpath("//span[@class='common_w-card_host-name']/text()").get();
            String viewer = html.xpath("//span[@class='common_w-card_views-num']/text()").get();
            String category = html.xpath("//a[@class='common_w-card_category']/text()").get();
            anchor.setRid(rid);
            anchor.setName(name);
            anchor.setTitle(title);
            anchor.setCategory(null == category ? "" : category);
            anchor.setPopularityStr(viewer);
            anchor.setPopularityNum(Integer.parseInt(viewer));
            anchor.setJob(job);
            anchor.setPlat(Const.QUANMIN);
            anchor.setGame(Const.GAMEALL);
            anchor.setUrl(curUrl);
            resultSetStr.add(anchor.toString());
        }


//        List<String> rids = page.getHtml().xpath("//div[@class='list_w-videos'][2]/ul[@class='list_w-videos_video-list']/li/div/div/a[@class='common_w-card_href']/@href").all().stream().map(url -> url.substring(url.lastIndexOf("/") + 1)).collect(Collectors.toList());
//        List<String> titles = page.getHtml().xpath("//div[@class='list_w-videos'][2]/ul[@class='list_w-videos_video-list']/li/div/div/a[@class='common_w-card_href']/div[@class='common_w-card_bottom']/div/p/text()").all();
//        List<String> names = page.getHtml().xpath("//div[@class='list_w-videos'][2]/ul[@class='list_w-videos_video-list']/li/div/div/a[@class='common_w-card_href']/div[@class='common_w-card_bottom']/div/div/span[@class='common_w-card_host-name']/text()").all();
//        List<String> viewers = page.getHtml().xpath("//div[@class='list_w-videos'][2]/ul[@class='list_w-videos_video-list']/li/div/div/a[@class='common_w-card_href']/div[@class='common_w-card_bottom']/div/div/span[@class='common_w-card_views-num']/text()").all();
//        List<String> categories = page.getHtml().xpath("//div[@class='list_w-videos'][2]/ul[@class='list_w-videos_video-list']/li/div/div/a[@class='common_w-card_category']/text()").all();
//        for (int i = 0; i < rids.size(); i++) {
//            Anchor anchor = new Anchor();
//            anchor.setRid(rids.get(i));
//            anchor.setName(names.get(i));
//            anchor.setTitle(titles.get(i));
//            anchor.setCategory(categories.get(i));
//            anchor.setPopularityStr(viewers.get(i));
//            anchor.setPopularityNum(Integer.parseInt(viewers.get(i)));
//            anchor.setJob(job);
//            anchor.setPlat(Const.QUANMIN);
//            anchor.setGame(Const.GAMEALL);
//            anchor.setUrl(curUrl);
////            new Thread(new Runnable() {
////                @Override
////                public void run() {
////                    HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
////                            .append("&par_d=").append(date).append(anchor.toString()).toString());
////                }
////            }).start();
////            try {
////                Thread.sleep(3);
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
//            resultSetStr.add(anchor.toString());
//        }
    }


    @Override
    public Site getSite() {
        super.getSite();
        return this.site;
    }

    public static void crawler(String[] args) {
        job = args[0];//quanminanchor
        date = args[1];//20161114
        hour = args[2];//10
        Const.GENERATORKEY = "panda";
        Const.GENERATORPASS = "pandatvpassw0rd";
        initParam(args);
        //钩子
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutDownHook()));

        long start = System.currentTimeMillis();
        Spider.create(new QuanminAnchorProcessor()).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000 + 1;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());

        executeResults();
    }
}
