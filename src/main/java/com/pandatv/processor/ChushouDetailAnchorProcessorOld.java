package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class ChushouDetailAnchorProcessorOld extends PandaProcessor {
    private static String urlTmp = "https://chushou.tv/live/down-v2.htm?&breakpoint=";
    private static String pointUrlTmpPre = "https://chushou.tv/play-help/bang-guide-info.htm?roomId=";
    private static String pointUrlTmpSuf = "&_=";//13位时间戳
    private static final Map<String, DetailAnchor> map = new HashMap<>();
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static final Logger logger = LoggerFactory.getLogger(ChushouDetailAnchorProcessorOld.class);
    private static int exCnt;
    private static final Set<String> weightFollowRids = new HashSet<>();
    private static int pageIndex = 0;

    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().get();
        logger.info("url:" + curUrl);
        try {
            if (curUrl.startsWith(urlTmp)) {
                String json = page.getJson().toString();
                JSONArray items = JsonPath.read(json, "$.data.items");
                String breakpoint = JsonPath.read(json, "$.data.breakpoint");
                if (items.size() > 0 && map.size() < Const.CHUSHOUMAX && pageIndex++ <= 500) {
                    page.addTargetRequest(urlTmp + breakpoint);
                    JSONArray read = JsonPath.read(json, "$.data.items");
                    for (int i = 0; i < read.size(); i++) {
                        String room = read.get(i).toString();
                        DetailAnchor detailAnchor = new DetailAnchor();
                        String rid = JsonPath.read(room, "$.targetKey");
                        detailAnchor.setRid(rid);
                        detailAnchor.setName(JsonPath.read(room, "$.meta.creator").toString());
                        detailAnchor.setTitle(JsonPath.read(room, "$.name").toString());
                        detailAnchor.setCategorySec(JsonPath.read(room, "$.meta.gameName").toString());
                        int populary = JsonPath.read(room, "$.meta.onlineCount");
                        detailAnchor.setViewerNum(populary);
                        int follow = JsonPath.read(room, "$.meta.subscriberCount");
                        detailAnchor.setFollowerNum(follow);
                        detailAnchor.setJob(job);
                        detailAnchor.setUrl(curUrl);
                        page.addTargetRequest(new Request(pointUrlTmpPre + rid + pointUrlTmpSuf + new Date().getTime()).putExtra("rid", rid));
                        map.put(rid, detailAnchor);
                    }
                } else {
                    page.setSkip(true);
                }
            } else if (curUrl.equals("https://chushou.tv/live/list.htm")) {
                String breakPoint = page.getHtml().xpath("//div[@class='more']/@data-break").toString();
                page.addTargetRequest(urlTmp + breakPoint);
                Html html = page.getHtml();
                List<String> urls = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='home_live_block']/a/@href").all();
                List<String> titles = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/a/text()").all();
                List<String> names = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='liveDetail']/span[@class='livePlayerName]/text()").all();
                List<String> categories = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='liveDetail']/a[@class='game_Name]/text()").all();
                for (int i = 0; i < urls.size(); i++) {
                    String rid = urls.get(i);
                    String title = titles.get(i);
                    String name = names.get(i);
                    String category = categories.get(i);
                    page.addTargetRequest(new Request(rid).putExtra("title", title).putExtra("name", name).putExtra("category", category));
                }
            } else if (curUrl.startsWith("https://chushou.tv/play-help/bang-guide-info.htm")) {
                String json = page.getJson().toString();
                Integer point = JsonPath.read(json, "$.data.current.point");
                Long lastDate = JsonPath.read(json, "$.data.last.date");
                String rid = (String) page.getRequest().getExtra("rid");
                DetailAnchor detailAnchor = map.get(rid);
                detailAnchor.setWeightNum(null == point ? 0 : point);
                detailAnchor.setLastStartTime(null == lastDate ? null : format.format(lastDate / 1000));
                weightFollowRids.add(rid);
            } else if (curUrl.contains("://chushou.tv/room/")) {
                Html html = page.getHtml();
                DetailAnchor detailAnchor = new DetailAnchor();
                String rid = html.xpath("//span[@class='roomnumber']/@title").get();
                String name = page.getRequest().getExtra("name").toString();
                String title = page.getRequest().getExtra("title").toString();
                String category = page.getRequest().getExtra("category").toString();
                String onlineStr = html.xpath("//span[@class='onlineCount']/text()").get();
                String followStr = html.xpath("//div[@class='zb_attention_left']/@data-subscribercount").get();
                detailAnchor.setRid(rid);
                detailAnchor.setName(name);
                detailAnchor.setTitle(title);
                detailAnchor.setCategorySec(category);
                detailAnchor.setViewerNum(Integer.parseInt(onlineStr));
                detailAnchor.setFollowerNum(Integer.parseInt(followStr));
                detailAnchor.setJob(job);
                detailAnchor.setUrl(curUrl);
                map.put(rid, detailAnchor);
                page.addTargetRequest(new Request(pointUrlTmpPre + rid + pointUrlTmpSuf + new Date().getTime()).putExtra("rid", rid));
            }
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.error("execute faild,url:" + curUrl);
            e.printStackTrace();
            if (++exCnt % 100 == 0) {
                MailTools.sendAlarmmail("huyadetailanchor 异常请求个数过多", "url: " + failedUrl.toString());
//                System.exit(1);
            }
        }

    }

    @Override
    public Site getSite() {
        super.getSite();
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "https://chushou.tv/live/list.htm";
        job = args[0];//chushouanchor
        date = args[1];//20161114
        hour = args[2];//10
        thread = 4;
        initParam(args);
        String hivePaht = Const.COMPETITORDIR + "crawler_detail_anchor/" + date;
        //钩子
        Runtime.getRuntime().addShutdownHook(new Thread(new ChushouDetailShutDownHook()));

        long start = System.currentTimeMillis();
        Spider.create(new ChushouDetailAnchorProcessorOld()).thread(thread).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000 + 1;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
//        for (String rid : weightFollowRids) {
//            resultSetStr.add(map.get(rid).toString());
//            DetailAnchor da = map.get(rid);
//            try {
//                new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.DETAILANCHOREVENT)
//                                .append("&par_d=").append(date).append(da.toString()).toString());
//                    }
//                }).start();
//                Thread.sleep(10);
//                    .append("&rid=").append(rid)
//                    .append("&nm=").append(CommonTools.getFormatStr(da.getName()))
//                    .append("&tt=").append(CommonTools.getFormatStr(da.getTitle()))
//                    .append("&cate_fir=&cate_sec=").append(da.getCategorySec())
//                    .append("&on_num=").append(da.getViewerNum())
//                    .append("&fol_num=").append(da.getFollowerNum())
//                    .append("&task=").append(job)
//                    .append("&rank=&w_str=&w_num=").append(da.getWeightNum())
//                    .append("&tag=&url=").append(da.getUrl())
//                    .append("&c_time=").append(createTimeFormat.format(new Date()))
//                    .append("&notice=&last_s_t=").append(da.getLastStartTime())
//                    .append("&t_ran=").append(PandaProcessor.getRandomStr()).toString());

//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        CommonTools.writeAndMail(hivePaht, Const.CHUSHOUFINISHDETAIL, detailAnchors);


        executeMapResults();
    }

    private static void executeMapResults() {
        for (String rid : weightFollowRids) {
            resultSetStr.add(map.get(rid).toString());
        }
        String dirFile = new StringBuffer(Const.CRAWLER_DATA_DIR).append(date).append("/").append(hour).append("/").append(job).append("_").append(date).append("_").append(hour).append(randomStr).toString();
        CommonTools.write2Local(dirFile, resultSetStr);
    }

    private static class ChushouDetailShutDownHook implements Runnable {
        @Override
        public void run() {
            logger.info("writeSuccess:"+writeSuccess);
            if (!writeSuccess){
                executeMapResults();
            }
        }
    }
}
