package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.HttpUtil;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

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
    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            String json = page.getJson().toString();
            if (StringUtils.isEmpty(json)) {
                return;
            }
            if (curUrl.startsWith("http://www.quanmin.tv/json/play/list.json?_t=")) {
                int pageCount = JsonPath.read(json, "$.pageCount");
                if (pageCount > 1) {
                    String addUrl = urlTmp + 2 + urlJsonT + format.format(new Date());
                    page.addTargetRequest(addUrl);
                } else {
                    page.setSkip(true);
                }
            } else if (curUrl.startsWith("http://www.quanmin.tv/json/play/list_")) {
                int pageCount = JsonPath.read(json, "$.pageCount");
                int curPage = Integer.parseInt(curUrl.substring(curUrl.indexOf("list_") + 5, curUrl.indexOf(".json")));
                if (curPage < pageCount) {
                    String addUrl = urlTmp + (curPage + 1) + urlJsonT + format.format(new Date());
                    page.addTargetRequest(addUrl);
                    JSONArray data = JsonPath.read(json, "$.data");
                    for (int i = 0; i < data.size(); i++) {
                        String no = JsonPath.read(data.get(i), "$.no");
                        page.addTargetRequest(new Request(detailUrlPre + no + detailUrlSuf));
                    }
                } else {
                    page.setSkip(true);
                }
            } else if (curUrl.startsWith("http://www.quanmin.tv/json/rooms/")) {
                int rid = JsonPath.read(json, "$.uid");
                String name = JsonPath.read(json, "$.nick");
                String title = JsonPath.read(json, "$.title");
                String category = JsonPath.read(json, "$.category_name");
                int online = JsonPath.read(json, "$.view");
                int follow = JsonPath.read(json, "$.follow");
                int weight = JsonPath.read(json, "$.weight");//统计的时候需要除以100
                String lastStartTime = JsonPath.read(json, "$.play_at");//substring(0,16)
                HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.DETAILANCHOREVENT)
                        .append("&par_d=").append(date)
                        .append("&rid=").append(rid)
                        .append("&nm=").append(CommonTools.getFormatStr(name))
                        .append("&tt=").append(CommonTools.getFormatStr(title))
                        .append("&cate_fir=&cate_sec=").append(category)
                        .append("&on_num=").append(online)
                        .append("&fol_num=").append(follow)
                        .append("&task=").append(job)
                        .append("&rank=&w_str=&w_num=").append((int) (weight / 100))
                        .append("&tag=&url=").append(curUrl)
                        .append("&c_time=").append(createTimeFormat.format(new Date()))
                        .append("&notice=&last_s_t=").append(lastStartTime.substring(0, 16))
                        .append("&t_ran=").append(PandaProcessor.getRandomStr()).toString());
//                DetailAnchor detailAnchor = new DetailAnchor();
//                detailAnchor.setRid(rid + "");
//                detailAnchor.setName(name);
//                detailAnchor.setTitle(title);
//                detailAnchor.setCategorySec(category);
//                detailAnchor.setViewerNum(online);
//                detailAnchor.setFollowerNum(follow);
//                detailAnchor.setWeightNum((int) (weight / 100));
//                detailAnchor.setLastStartTime(lastStartTime.substring(0, 16));
//                detailAnchor.setJob(job);
//                detailAnchor.setUrl(curUrl);
//                detailAnchorObjs.add(detailAnchor);
            }
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{}" + curUrl);
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail(Const.DOUYUEXIT, "url: " + curUrl);
                System.exit(1);
            }
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "http://www.quanmin.tv/json/play/list.json?_t=";
        job = args[0];//quanminanchor
        date = args[1];//20161114
        hour = args[2];//10
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_detail_anchor/" + date;
        String dateStr = format.format(new Date());
        long start = System.currentTimeMillis();
        Spider.create(new QuanminDetailAnchorProcessor()).thread(2).addUrl(firUrl + dateStr).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs));
//        for (DetailAnchor detailAnchor : detailAnchorObjs) {
//            detailAnchors.add(detailAnchor.toString());
//        }
//        CommonTools.writeAndMail(hivePaht, Const.QUANMINFINISHDETAIL, detailAnchors);
    }
}
