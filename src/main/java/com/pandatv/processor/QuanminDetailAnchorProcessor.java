package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.HttpUtil;
import com.pandatv.tools.MailTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class QuanminDetailAnchorProcessor extends PandaProcessor {
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
    private static String urlTmp = "http://www.quanmin.tv/json/play/list_";
    private static final String urlJsonT = ".json?_t=";
    private static final String detailUrlPre = "http://www.quanmin.tv/json/rooms/";
    private static final String detailUrlSuf = "/noinfo4.json";
    private static final Logger logger = LoggerFactory.getLogger(QuanminDetailAnchorProcessor.class);
    private static int exCnt;

    private static String firUrl = "https://www.quanmin.tv/game/all";

    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            if (curUrl.equals(firUrl)) {
                List<String> urls = page.getHtml().xpath("//div[@class='list_w-videos_paging']/a/@href").all();
                int maxPage = urls.stream().filter(url -> url.contains("all?p=")).map(url -> Integer.parseInt(url.substring(url.lastIndexOf("=") + 1).trim())).reduce((a, b) -> a > b ? a : b).get();
                if (maxPage > 1) {
                    IntStream.range(2, maxPage).forEach(p -> page.addTargetRequest(firUrl + "?p=" + p));
                }
                page.getHtml().xpath("//div[@class='list_w-videos'][2]/ul[@class='list_w-videos_video-list']/li/div/div/a[@class='common_w-card_href']/@href").all().stream().forEach(url -> page.addTargetRequest(url));
            } else if (curUrl.startsWith(firUrl)) {
                page.getHtml().xpath("//div[@class='list_w-videos'][2]/ul[@class='list_w-videos_video-list']/li/div/div/a[@class='common_w-card_href']/@href").all().stream().forEach(url -> page.addTargetRequest(url));
            } else {
                parseDetailAnchor(page, curUrl);
            }
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{}" + curUrl);
            e.printStackTrace();
            if (++exCnt % 300 == 0) {
                MailTools.sendAlarmmail("quanmindetailanchor 异常请求个数过多", "url: " + curUrl);
//                System.exit(1);
            }
        }
    }

    private void parseDetailAnchor(Page page, String curUrl) {
        String rid = page.getHtml().xpath("//div[@class='room_w-title_right']/div/span[2]/span/text()").get();
        String name = page.getHtml().xpath("//div[@class='room_w-title_right']/div/span[1]/h2/text()").get();
        String viewers = page.getHtml().xpath("//div[@class='room_w-title_right']/div/span[3]/span/text()").get();
        String weight = page.getHtml().xpath("//div[@class='room_w-title_right']/div/span[4]/span/text()").get();
        String followers = page.getHtml().xpath("//div[@class='room_w-title-tool']/div[1]/span/text()").get();
        String title = page.getHtml().xpath("//div[@class='room_w-title_right']/h2/span/text()").get();
        String category = page.getHtml().xpath("//div[@class='room_w-title_right']/h2/a/text()").get();
        DetailAnchor detailAnchor = new DetailAnchor();
        detailAnchor.setRid(rid + "");
        detailAnchor.setName(name);
        detailAnchor.setTitle(title);
        detailAnchor.setCategorySec(category);
        detailAnchor.setViewerNum(Integer.parseInt(viewers));
        detailAnchor.setFollowerNum(Integer.parseInt(followers.replace(",", "")));
        detailAnchor.setWeightNum(getQuanminWeight(weight.replace(",", "").trim()));
        detailAnchor.setJob(job);
        detailAnchor.setUrl(curUrl);
        HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.DETAILANCHOREVENT)
                .append("&par_d=").append(date).append(detailAnchor.toString()).toString());
    }

    private long getQuanminWeight(String weight) {
        if (weight.contains("万")) {
            return (long) (Double.parseDouble(weight.replace("万", "")) * 10000);
        }
        return (long) Double.parseDouble(weight);
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        job = args[0];//quanminanchor
        date = args[1];//20161114
        hour = args[2];//10
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        long start = System.currentTimeMillis();
        Spider.create(new QuanminDetailAnchorProcessor()).thread(3).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
    }
}
