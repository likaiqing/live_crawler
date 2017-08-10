package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.HttpUtil;
import com.pandatv.tools.MailTools;
import com.pandatv.tools.UnicodeTools;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by likaiqing on 2016/11/16.
 */
public class HuyaDetailAnchorProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HuyaDetailAnchorProcessor.class);
    private static String tmpUrl = "http://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&tagAll=0&page=";
    private static String tmpHostUrl = "http://www.huya.com/";
    private static Set<String> competitionLive = new HashSet<>();
    private static String competitionUrl = "http://www.huya.com/cache.php?m=HotRecApi&do=getLiveInfo&yyid=";
    private static int exCnt;
    private static String huyaDomain = "http://www.huya.com/";
    private static Pattern isNotlivd = Pattern.compile("\"isNotLive\" : \"(\\d)\",");
    int i = 0;

    @Override
    public void process(Page page) {
        requests++;
        synchronized (this) {
            i++;
        }
        String curUrl = page.getUrl().toString();
        logger.info("process url:{}", curUrl);
        try {
            if (curUrl.startsWith(tmpUrl)) {
                String json = page.getJson().get();
                int totalPage = JsonPath.read(json, "$.data.totalPage");
                int curPage = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf('=') + 1));
                int newPage = curPage + 1;
                if (curPage < totalPage) {
                    page.addTargetRequest(tmpUrl + newPage);
                    List<String> rooms = page.getJson().jsonPath("$.data.datas").all();
                    for (String room : rooms) {
                        String privateHost = JsonPath.read(room, "$.privateHost").toString();
                        Request request = new Request(huyaDomain + privateHost);
                        request.putExtra("rid", privateHost);
                        page.addTargetRequest(request);
                    }
                }

            } else {
                Html html = page.getHtml();
                String rid = page.getRequest().getExtra("rid").toString();
                String name = html.xpath("//span[@class='host-name']/text()").get();
                String title = html.xpath("//h1[@class='host-title']/text()").get();
                String categoryFir = "";
                String categorySec = "";
                List<String> category = html.xpath("//span[@class='host-channel']/a/text()").all();
                if (category.size() == 2) {
                    categoryFir = category.get(0);
                    categorySec = category.get(1);
                } else if (category.size() == 1) {
                    categoryFir = category.get(0);
                    categorySec = category.get(0);
                }
                String viewerStr = html.xpath("//span[@class='host-spectator']/em/text()").get();
                if (!StringUtils.isEmpty(viewerStr) && viewerStr.contains(",")) {
                    viewerStr = viewerStr.replace(",", "");
                }
                String followerStr = html.xpath("//div[@id='activityCount']/text()").get();
//                String tag = html.xpath("//span[@class='host-channel']/a/text()").all().toString();//逗号分隔
//                String notice = html.xpath("//div[@class='notice-cont']/text()").get();
//                HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.DETAILANCHOREVENT)
//                        .append("&par_d=").append(date)
//                        .append("&rid=").append(rid)
//                        .append("&nm=").append(CommonTools.getFormatStr(name))
//                        .append("&tt=").append(CommonTools.getFormatStr(title))
//                        .append("&cate_fir=").append(categoryFir)
//                        .append("&cate_sec=").append(categorySec)
//                        .append("&on_num=").append(StringUtils.isEmpty(viewerStr) ? 0 : Integer.parseInt(viewerStr))
//                        .append("&fol_num=").append(StringUtils.isEmpty(followerStr) ? 0 : Integer.parseInt(followerStr))
//                        .append("&task=").append(job)
//                        .append("&rank=&w_str=&w_num=&tag=&url=").append(curUrl)
//                        .append("&c_time=").append(createTimeFormat.format(new Date()))
//                        .append("&notice=&last_s_t=&t_ran=").append(PandaProcessor.getRandomStr()).toString());
                DetailAnchor detailAnchor = new DetailAnchor();
                detailAnchor.setRid(rid);
                detailAnchor.setName(name);
                detailAnchor.setTitle(title);
                detailAnchor.setCategoryFir(categoryFir);
                detailAnchor.setCategorySec(categorySec);
                detailAnchor.setViewerNum(StringUtils.isEmpty(viewerStr) ? 0 : Integer.parseInt(viewerStr));
                detailAnchor.setFollowerNum(StringUtils.isEmpty(followerStr) ? 0 : Integer.parseInt(followerStr));
                detailAnchor.setTag("");
                detailAnchor.setNotice("");
                detailAnchor.setJob(job);
                detailAnchor.setUrl(curUrl);
//                new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.DETAILANCHOREVENT)
//                                .append("&par_d=").append(date).append(detailAnchor.toString()).toString());
//                    }
//                }).start();
//                Thread.sleep(3);
//                detailAnchorObjs.add(detailAnchor);
                resultSetStr.add(detailAnchor.toString());
                page.setSkip(true);
            }
        } catch (Exception e) {
            if (competitionLive.contains(curUrl)) {
                String rid = curUrl.substring(curUrl.lastIndexOf("/") + 1);
                Request request = new Request(competitionUrl + rid).setPriority(5);
                request.putExtra("rid", rid);
                page.addTargetRequest(request);
            } else {
                failedUrl.append(curUrl + ";");
            }
            logger.info("process exception,url:{}" + curUrl);
            e.printStackTrace();
            if (++exCnt % 600==0) {
                MailTools.sendAlarmmail("huyadetailanchor 异常请求个数过多", "url: " + failedUrl.toString());
//                System.exit(1);
            }
        }
        page.setSkip(true);
    }

    @Override
    public Site getSite() {
        return this.site.setSleepTime(1);
    }

    public static void crawler(String[] args) {
        competitionLive.add("http://www.huya.com/1584989003");
        competitionLive.add("http://www.huya.com/1735596609");
        competitionLive.add("http://www.huya.com/1735597169");
        competitionLive.add("http://www.huya.com/1773588838");
        job = args[0];//
        date = args[1];
        hour = args[2];
        Const.GENERATORKEY = "H05972909IM78TAP";
        Const.GENERATORPASS = "36F7B5D8703A39C5";
        thread = 5;
        if (args.length == 4) {
            thread = Integer.parseInt(args[3]);
        }
        String firstUrl = "http://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&tagAll=0&page=1";
        String hivePaht = Const.COMPETITORDIR + "crawler_detail_anchor/" + date;
        long start = System.currentTimeMillis();
        Spider.create(new HuyaDetailAnchorProcessor()).thread(thread).addUrl(tmpUrl + 1).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs)+ ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
//        for (DetailAnchor detailAnchor : detailAnchorObjs) {
//            detailAnchors.add(detailAnchor.toString());
//        }
//        logger.info("时间:" + date + " " + hour + "");
//        CommonTools.writeAndMail(hivePaht, Const.HUYAFINISHDETAIL, detailAnchors);

        String dirFile = new StringBuffer(Const.CRAWLER_DATA_DIR).append(date).append("/").append(hour).append("/").append(job).append("_").append(date).append("_").append(hour).append(randomStr).toString();
        CommonTools.write2Local(dirFile,resultSetStr);
    }
}
