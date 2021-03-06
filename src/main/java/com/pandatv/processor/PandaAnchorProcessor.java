package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.HttpUtil;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

/**
 * Created by likaiqing on 2016/12/14.
 */
public class PandaAnchorProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(PandaAnchorProcessor.class);
    private static int exCnt;

    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            JSONArray items = JsonPath.read(page.getJson().get(), "$.data.items");
            if (items.size() > 0) {
                int equalIndex = curUrl.lastIndexOf("=");
                int curPage = Integer.parseInt(curUrl.substring(equalIndex + 1));
                if (curPage > 150) {
                    return;
                }
                page.addTargetRequest(curUrl.substring(0, equalIndex) + "=" + (curPage + 1));
                for (Object obj : items) {
                    String rid = JsonPath.read(obj, "$.id");
                    String name = JsonPath.read(obj, "$.userinfo.nickName");
                    String title = JsonPath.read(obj, "$.name");
                    String category = JsonPath.read(obj, "$.classification.cname");
                    String popularitiyStr = JsonPath.read(obj, "$.person_num");
                    int popularitiyNum = Integer.parseInt(popularitiyStr);
//                    HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
//                            .append("&par_d=").append(date)
//                            .append("&rid=").append(rid)
//                            .append("&nm=").append(CommonTools.getFormatStr(name))
//                            .append("&tt=").append(CommonTools.getFormatStr(title))
//                            .append("&cate=").append(category)
//                            .append("&pop_s=").append(popularitiyStr)
//                            .append("&pop_n=").append(popularitiyNum)
//                            .append("&task=").append(job)
//                            .append("&plat=").append(Const.PANDA)
//                            .append("&url_c=").append(Const.GAMEALL)
//                            .append("&c_time=").append(createTimeFormat.format(new Date()))
//                            .append("&url=").append(curUrl)
//                            .append("&t_ran=").append(PandaProcessor.getRandomStr()).toString());
                    Anchor anchor = new Anchor();
                    anchor.setRid(rid);
                    anchor.setName(name);
                    anchor.setTitle(title);
                    anchor.setCategory(category);
                    anchor.setPopularityStr(popularitiyStr);
                    anchor.setPopularityNum(popularitiyNum);
                    anchor.setJob(job);
                    anchor.setPlat(Const.PANDA);
                    anchor.setGame(Const.GAMEALL);
                    anchor.setUrl(curUrl);
//                    new Thread(new Runnable() {
//                        @Override
//                        public void run() {
//                            HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
//                                    .append("&par_d=").append(date).append(anchor.toString()).toString());
//                        }
//                    }).start();
//                    Thread.sleep(3);
//                    anchorObjs.add(anchor);
                    resultSetStr.add(anchor.toString());
                }
            }
            page.setSkip(true);
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{}" + curUrl);
            e.printStackTrace();
            if (++exCnt % 10 == 0) {
                MailTools.sendAlarmmail("pandaanchor 异常请求个数过多", "url: " + failedUrl.toString());
//                System.exit(1);
            }
        }
    }

    @Override
    public Site getSite() {
        return this.site.setHttpProxy(null);
    }

    public static void crawler(String[] args) {
        String firUrl = "https://www.panda.tv/live_lists?status=2&order=person_num&pagenum=120&pageno=1";
        job = args[0];//pandaanchor
        date = args[1];//20161114
        hour = args[2];//10
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
//        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        //钩子
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutDownHook()));

        long start = System.currentTimeMillis();
        Spider.create(new PandaAnchorProcessor()).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000 + 1;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
//        for (Anchor anchor : anchorObjs) {
//            anchors.add(anchor.toString());
//        }
//        CommonTools.writeAndMail(hivePaht, Const.PANDAANCHORFINISH, anchors);


        executeResults();
    }
}
