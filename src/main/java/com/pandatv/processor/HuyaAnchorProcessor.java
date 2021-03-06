package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.util.List;

/**
 * Created by likaiqing on 2016/11/11.
 */
public class HuyaAnchorProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HuyaAnchorProcessor.class);
    //    private static String url = "http://www.huya.com/cache.php?m=Live&do=ajaxAllLiveByPage&pageNum=1&page=";
    private static String url = "http://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&tagAll=0&page=";
    private static int exCnt;
    private static String firstUrl = "http://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&tagAll=0&page=1";

    @Override
    public void process(Page page) {
        super.getSite();
        requests++;
        String url = page.getUrl().toString();
        try {
            logger.info("process url:{}", url);
            if (url.equals(firstUrl)) {
                int totalPage = JsonPath.read(page.getJson().get(), "$.data.totalPage");
                for (int i = 2; i <= totalPage; i++) {
                    page.addTargetRequest(this.url + i);
                }
                executeDate(page);
            } else {
                executeDate(page);
            }
        } catch (Exception e) {
            failedUrl.append(url + ";  ");
            logger.info("process exception,url:{}" + url);
            e.printStackTrace();
            if (++exCnt % 10 == 0) {
                MailTools.sendAlarmmail("huyaanchor 异常请求个数过多", "url: " + failedUrl.toString());
//                System.exit(1);
            }
        }

    }

    private void executeDate(Page page) {
        List<String> all = page.getJson().jsonPath("$.data.datas").all();
        if (all.size() > 0) {
            JSONArray list = JsonPath.read(page.getJson().get(), "$.data.datas");
            for (int i = 0; i < list.size(); i++) {
                String jsonStr = list.get(i).toString();
                String rid = JsonPath.read(jsonStr, "$.privateHost");
                String name = JsonPath.read(jsonStr, "$.nick");
                String title = JsonPath.read(jsonStr, "$.introduction");
                String category = JsonPath.read(jsonStr, "$.gameFullName");
                String popularityStr = JsonPath.read(jsonStr, "$.totalCount");
                int popularityNum = Integer.parseInt(popularityStr);
//                    HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
//                            .append("&par_d=").append(date)
//                            .append("&rid=").append(rid)
//                            .append("&nm=").append(CommonTools.getFormatStr(name))
//                            .append("&tt=").append(CommonTools.getFormatStr(title))
//                            .append("&cate=").append(category)
//                            .append("&pop_s=").append(popularityStr)
//                            .append("&pop_n=").append(popularityNum)
//                            .append("&task=").append(job)
//                            .append("&plat=").append(Const.HUYA)
//                            .append("&url_c=").append(Const.GAMEALL)
//                            .append("&c_time=").append(createTimeFormat.format(new Date()))
//                            .append("&url=").append(url)
//                            .append("&t_ran=").append(PandaProcessor.getRandomStr()).toString());
                Anchor anchor = new Anchor();
                anchor.setRid(rid);
                anchor.setName(name);
                anchor.setTitle(title);
                anchor.setCategory(category);
                anchor.setPopularityStr(popularityStr);
                anchor.setPopularityNum(popularityNum);
                anchor.setJob(job);
                anchor.setPlat(Const.HUYA);
                anchor.setGame(Const.GAMEALL);
                anchor.setUrl(url);
//                new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
//                                .append("&par_d=").append(date).append(anchor.toString()).toString());
//                    }
//                }).start();
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                    anchorObjs.add(anchor);
                resultSetStr.add(anchor.toString());
            }
        }
    }

    @Override
    public Site getSite() {
        super.getSite();
        return site;
    }

    public static void crawler(String[] args) {
        job = args[0];//huyaanchor
        date = args[1];
        hour = args[2];
        thread = 3;
        initParam(args);
        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        //钩子
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutDownHook()));

        long start = System.currentTimeMillis();
        Spider.create(new HuyaAnchorProcessor()).thread(thread).addUrl(firstUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000 + 1;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
//        for (Anchor anchor : anchorObjs) {
//            anchors.add(anchor.toString());
//        }
//        CommonTools.writeAndMail(hivePaht, Const.HUYAFINISH, anchors);

        executeResults();
    }

    public static void main(String[] args) {
        args = new String[]{"huyaanchor", "20161111", "16"};
        crawler(args);
    }
}
