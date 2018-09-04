package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.util.List;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class ChushouDetailAnchorProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ChushouDetailAnchorProcessor.class);
    private static final String firstUrl = "https://chushou.tv/livezone.htm";//链接版区页第一页
    private static final String nextCatePre = "https://chushou.tv/livezone/down.htm?breakpoint=";//链接版区页第二页及以后的
    private static final String cateUrlPre = "https://chushou.tv/nav-list.htm?targetKey=";//链接版区页第二页及以后的
    private static final String nextRoomListPre = "https://chushou.tv/nav-list/down.htm?targetKey=";//链接版区页第二页及以后的
    private static final String roomDetailPre = "https://chushou.tv/room/";//链接版区页第二页及以后的
    private static int exCnt;
    private static int cnt;

    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            if (curUrl.equals(firstUrl)) {
                Html html = page.getHtml();
                List<String> urls = html.xpath("//div[@class='gamezone-areas-con']/a/@href").all();
                page.addTargetRequests(urls);//单个版区房间列表firsturl
                String firBreakPoint = html.xpath("//div[@id='gamezone-areas-container']/@data-breakpoint").get();
                page.addTargetRequest(nextCatePre + firBreakPoint);//添加下一页版区列表url
            } else if (curUrl.startsWith(nextCatePre)) {
                String json = page.getJson().toString();
                JSONArray items = JsonPath.read(json, "$.data.items");
                if (items.size() > 0) {
                    String breakpoint = JsonPath.read(json, "$.data.breakpoint");
                    page.addTargetRequest(nextCatePre + breakpoint);
                    for (int i = 0; i < items.size(); i++) {
                        String targetKey = JsonPath.read(items.get(i), "$.targetKey").toString();
                        page.addTargetRequest(cateUrlPre + targetKey);
                    }
                }
            } else if (curUrl.startsWith(cateUrlPre)) {
                Html html = page.getHtml();
                List<String> detailUrls = html.xpath("//div[@id='liveContent']/a/@href").all();
                page.addTargetRequests(detailUrls);
                String dataBreak = html.xpath("//div[@class='more']/@data-break").get();
                String target = html.xpath("//div[@class='more']/@data-target-key").get();
                page.addTargetRequest(new StringBuffer(nextRoomListPre).append(target).append("&").append("breakpoint=").append(dataBreak).toString());
            } else if (curUrl.startsWith(nextRoomListPre)) {
                String json = page.getJson().toString();
                JSONArray items = JsonPath.read(json, "$.data.items");
                if (items.size() > 0) {
                    String cate = "";
                    String breakpoint = JsonPath.read(json, "$.data.breakpoint");
                    page.addTargetRequest(curUrl.substring(0, curUrl.lastIndexOf("=")) + breakpoint);
                    for (int i = 0; i < items.size(); i++) {
                        String title = JsonPath.read(items.get(i), "$.name").toString();
                        String name = JsonPath.read(items.get(i), "$.meta.creator").toString();
                        cate = JsonPath.read(items.get(i), "$.meta.gameName").toString();
                        String liveCnt = JsonPath.read(items.get(i), "$.meta.onlineCount").toString();
                        String subscriberCount = JsonPath.read(items.get(i), "$.meta.subscriberCount").toString();
                        String rid = JsonPath.read(items.get(i), "$.meta.roomId").toString();
                        DetailAnchor anchor = new DetailAnchor();
                        anchor.setRid(rid);
                        anchor.setName(name);
                        anchor.setTitle(title);
                        anchor.setCategorySec(cate);
                        anchor.setViewerNum(Integer.parseInt(liveCnt));
                        anchor.setFollowerNum(Integer.parseInt(subscriberCount));
                        anchor.setJob(job);
                        anchor.setUrl(curUrl);
                        resultSetStr.add(anchor.toString());
                    }
                }
            } else if (curUrl.startsWith(roomDetailPre)) {
                Html html = page.getHtml();
                DetailAnchor anchor = new DetailAnchor();
                anchor.setRid(curUrl.substring(curUrl.lastIndexOf("/") + 1, curUrl.lastIndexOf(".htm")));
                anchor.setName(html.xpath("//div[@id='isManager']/span[@id='sp1']/text()").get());
                anchor.setTitle(html.xpath("//div[@class='zb_player_ifm']/p[@class='zb_player_gamedesc']/@title").get());
                anchor.setCategorySec(html.xpath("//a[@id='sp2']/@title").get());
                anchor.setViewerNum(Integer.parseInt(html.xpath("//span[@class='onlineCount']/text()").get()));
                anchor.setFollowerNum(Integer.parseInt(html.xpath("//div[@class='zb_attention_left']/@data-subscribercount").get()));
                anchor.setJob(job);
                anchor.setUrl(curUrl);
                resultSetStr.add(anchor.toString());
            }
            page.setSkip(true);
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.error("execute faild,url:" + curUrl);
            e.printStackTrace();
            if (++exCnt % 10 == 0) {
                MailTools.sendAlarmmail("chushouanchor 异常请求个数过多", "url: " + failedUrl.toString());
//                System.exit(1);
            }
        }
    }

    private int getLiveNumber(String liveStr) {
        double liveNumber = 0.0;
        boolean contains = false;
        if (liveStr.contains("万")) {
            liveStr = liveStr.substring(0, liveStr.indexOf("万"));
            contains = true;
        }
        try {
            liveNumber = Double.parseDouble(liveStr);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("liveStr:" + liveStr);
        }
        if (contains) {
            liveNumber = liveNumber * 10000;
        }
        return (int) Math.ceil(liveNumber);
    }

    @Override
    public Site getSite() {
        super.getSite();
        return this.site.setSleepTime(0);
    }

    public static void crawler(String[] args) {
        job = args[0];//chushouanchor
        date = args[1];//20161114
        hour = args[2];//10
        thread = 1;
        initParam(args);
        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        //钩子
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutDownHook()));

        long start = System.currentTimeMillis();
        Spider.create(new ChushouDetailAnchorProcessor()).addUrl(firstUrl).thread(thread).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000 + 1;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
//        for (Anchor anchor : anchorObjs) {
//            anchors.add(anchor.toString());
//        }
//        CommonTools.writeAndMail(hivePaht, Const.CHUSHOUFINISH, anchors);

        executeResults();
    }
}
