package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by likaiqing on 2016/12/14.
 */
public class PandaDetailAnchorProcessor extends PandaProcessor {
    private static final Map<String, DetailAnchor> map = new HashMap<>();
    private static final String detailUrlTmp = "https://www.panda.tv/";
    private static final String followJsonPrefex = "https://www.panda.tv/room_followinfo?roomid=";
    private static final String v2DetailJsonPrefex = "https://www.panda.tv/api_room_v2?roomid=";
    private static final Logger logger = LoggerFactory.getLogger(HuyaDetailAnchorProcessor.class);
    private static int exCnt;
    private static final Set<String> weightRids = new HashSet<>();
    private static final Set<String> followRids = new HashSet<>();

    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            if (curUrl.startsWith("https://www.panda.tv/live_lists?status=2&order=person_num&pagenum=120&pageno=")) {
                JSONArray items = JsonPath.read(page.getJson().get(), "$.data.items");
                if (items.size() > 0) {
                    int equalIndex = curUrl.lastIndexOf("=");
                    int curPage = Integer.parseInt(curUrl.substring(equalIndex + 1));
                    page.addTargetRequest(curUrl.substring(0, equalIndex) + "=" + (curPage + 1));
                    for (Object obj : items) {
                        String rid = JsonPath.read(obj, "$.id");
                        String name = JsonPath.read(obj, "$.userinfo.nickName");
                        String title = JsonPath.read(obj, "$.name");
                        String category = JsonPath.read(obj, "$.classification.cname");
                        String popularitiyStr = JsonPath.read(obj, "$.person_num");
                        int popularitiyNum = Integer.parseInt(popularitiyStr);
                        DetailAnchor detailAnchor = new DetailAnchor();
                        detailAnchor.setRid(rid);
                        detailAnchor.setName(name);
                        detailAnchor.setTitle(title);
                        detailAnchor.setCategorySec(category);
                        detailAnchor.setViewerNum(popularitiyNum);
                        detailAnchor.setJob(job);
                        detailAnchor.setUrl(curUrl);
                        map.put(rid, detailAnchor);
                        page.addTargetRequest(new Request(detailUrlTmp + rid.trim()).putExtra("rid", rid));
                    }
                }
            } else if (curUrl.startsWith(followJsonPrefex)) {//获取订阅数
                String rid = page.getRequest().getExtra("rid").toString();
                DetailAnchor detailAnchor = map.get(rid);
                int follow = JsonPath.read(page.getJson().toString(), "$.data.fans");
                detailAnchor.setFollowerNum(follow);
                followRids.add(rid);
            } else if (curUrl.startsWith(v2DetailJsonPrefex)) {
                String jsonStr = page.getJson().get();
                String rid = page.getRequest().getExtra("rid").toString();
                String bambooStr = JsonPath.read(jsonStr, "$.data.hostinfo.bamboos").toString();
                DetailAnchor detailAnchor = map.get(rid);
                detailAnchor.setWeightNum(Long.parseLong(bambooStr));
                weightRids.add(rid);
            } else if (curUrl.startsWith(detailUrlTmp)) {//设置体重window._config_roominfo bamboos
                String rid = page.getRequest().getExtra("rid").toString();
                DetailAnchor detailAnchor = map.get(rid);
                Elements scripts = page.getHtml().getDocument().getElementsByTag("script");
                boolean has = false;
                for (Element script : scripts) {
                    if (script.toString().contains("window._config_roominfo")) {
                        String scrStr = script.toString();
                        int bamStart = scrStr.indexOf("\"bamboos\":\"") + 11;
                        int bamEnd = scrStr.indexOf("\"", bamStart);
                        String weightStr = scrStr.substring(bamStart, bamEnd);
                        detailAnchor.setWeightNum(Long.parseLong(weightStr));
                        has = true;
                        weightRids.add(rid);
                    }
                }
                if (!has) {//源码没有window._config_roominfo信息
                    page.addTargetRequest(new Request(v2DetailJsonPrefex + rid.trim()).putExtra("rid", rid));
                }
                page.addTargetRequest(new Request(followJsonPrefex + rid.trim()).putExtra("rid", rid));
            }
            page.setSkip(true);
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{}" + curUrl);
            e.printStackTrace();
            if (++exCnt % 500 == 0) {
                MailTools.sendAlarmmail("pandadetailanchor 异常请求个数过多", "url: " + failedUrl.toString());
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
        String hivePaht = Const.COMPETITORDIR + "crawler_detail_anchor/" + date;
        //钩子
        Runtime.getRuntime().addShutdownHook(new Thread(new PandaDetailShutDownHook()));

        long start = System.currentTimeMillis();
        Spider.create(new PandaDetailAnchorProcessor()).thread(3).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000 + 1;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
//        for (String rid : weightRids) {
//            if (followRids.contains(rid)) {
//                detailAnchors.add(map.get(rid).toString());
//                DetailAnchor da = map.get(rid);
//                HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.DETAILANCHOREVENT)
//                        .append("&par_d=").append(date)
//                        .append("&rid=").append(rid)
//                        .append("&nm=").append(CommonTools.getFormatStr(da.getName()))
//                        .append("&tt=").append(CommonTools.getFormatStr(da.getTitle()))
//                        .append("&cate_fir=&cate_sec=").append(da.getCategorySec())
//                        .append("&on_num=").append(da.getViewerNum())
//                        .append("&fol_num=").append(da.getFollowerNum())
//                        .append("&task=").append(job)
//                        .append("&rank=&w_str=&w_num=").append(da.getWeightNum())
//                        .append("&tag=&url=").append(da.getUrl())
//                        .append("&c_time=").append(createTimeFormat.format(new Date()))
//                        .append("&notice=&last_s_t=").append(da.getLastStartTime())
//                        .append("&t_ran=").append(PandaProcessor.getRandomStr()).toString());
//                try {
//                    new Thread(new Runnable() {
//                        @Override
//                        public void run() {
//                            HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.DETAILANCHOREVENT)
//                                    .append("&par_d=").append(date).append(da.toString()).toString());
//                        }
//                    }).start();
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                resultSetStr.add(map.get(rid).toString());
//            } else {
//                System.out.println(rid);
//            }
//        }
//        CommonTools.writeAndMail(hivePaht, Const.PANDAANCHORFINISHDETAIL, detailAnchors);

        executeMapResults();

    }

    private static void executeMapResults() {
        for (String rid : weightRids) {
            if (followRids.contains(rid)) {
                resultSetStr.add(map.get(rid).toString());
            } else {
                System.out.println(rid);
            }
        }
        String dirFile = new StringBuffer(Const.CRAWLER_DATA_DIR).append(date).append("/").append(hour).append("/").append(job).append("_").append(date).append("_").append(hour).append(randomStr).toString();
        CommonTools.write2Local(dirFile, resultSetStr);
    }

    private static class PandaDetailShutDownHook implements Runnable {
        @Override
        public void run() {
            logger.info("writeSuccess:"+writeSuccess);
            if (!writeSuccess){
                executeMapResults();
            }
        }
    }
}
