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
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.util.List;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class DouyuAnchorProccessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DouyuAnchorProccessor.class);
    private static String url = "https://www.douyu.com/directory/all?isAjax=1&page=";
    private static int exCnt;

    public static void crawler(String[] args) {
        job = args[0];//douyuanchor
        date = args[1];
        hour = args[2];
        thread = 2;
        initParam(args);
//        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        String firstUrl = "https://www.douyu.com/directory/all";
        //钩子
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutDownHook()));

        long start = System.currentTimeMillis();
        Spider.create(new DouyuAnchorProccessor()).addUrl(firstUrl).thread(thread).setDownloader(new PandaDownloader()).addPipeline(new ConsolePipeline()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000 + 1;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
//        for (Anchor anchor : anchorObjs) {
//            anchors.add(anchor.toString());
//        }
//        CommonTools.writeAndMail(hivePaht, Const.DOUYUFINISH, anchors);

        executeResults();
    }

    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().toString();
        try {
            logger.info("process url:{}", curUrl);
            if (curUrl.equals("https://www.douyu.com/directory/all")) {
                Elements elements = page.getHtml().getDocument().getElementsByAttributeValue("type", "text/javascript");
                int endPage = 1;
                for (int i = 0; i < elements.size(); i++) {
                    String element = elements.get(i).toString();
                    if (element.contains("count:")) {
                        endPage = Integer.parseInt(element.substring(element.indexOf("count:") + 8, element.lastIndexOf(',') - 1));
                        break;
                    }
                }
                for (int i = 1; i < endPage; i++) {
//                    page.addTargetRequest("https://www.douyu.com/directory/all?isAjax=1&page=" + i);
                    page.addTargetRequest("https://www.douyu.com/gapi/rkc/directory/0_0/" + i);
                }
                page.setSkip(true);
            } else {
//                List<String> rids = page.getHtml().xpath("//body/li/@data-rid").all();
//                List<String> names = page.getHtml().xpath("//body/li/a/div[@class='mes']/p/span[@class='dy-name ellipsis fl']/text()").all();
//                List<String> titles = page.getHtml().xpath("//body/li/a/@title").all();
//                List<String> popularities = page.getHtml().xpath("//body/li/a/div[@class='mes']/p/span[@class='dy-num fr']/text()").all();
//                List<String> categories = page.getHtml().xpath("//body/li/a/div[@class='mes']/div[@class='mes-tit']/span/text()").all();
//                for (int i = 0; i < names.size(); i++) {
//                    Anchor anchor = new Anchor();
//                    String popularitiyStr = popularities.get(i);
//                    int popularitiyNum = CommonTools.createNum(popularitiyStr);
//                    String rid = rids.get(i);
//                    if (rid.contains("\u0001")) {
//                        logger.info("rid contains SEP,url:{},rid:{}", url, rid);
//                    }
//                    if (!CommonTools.isValidUnicode(rid)) {
//                        logger.info("rid is not valid unicode,url:{},rid:{}", url, rid);
//                    }
//                    HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
//                            .append("&par_d=").append(date)
//                            .append("&rid=").append(rid)
//                            .append("&nm=").append(CommonTools.getFormatStr(names.get(i)))
//                            .append("&tt=").append(CommonTools.getFormatStr(titles.get(i)))
//                            .append("&cate=").append(categories.get(i))
//                            .append("&pop_s=").append(popularitiyStr)
//                            .append("&pop_n=").append(popularitiyNum)
//                            .append("&task=").append(job)
//                            .append("&plat=").append(Const.DOUYU)
//                            .append("&url_c=").append(Const.GAMEALL)
//                            .append("&c_time=").append(createTimeFormat.format(new Date()))
//                            .append("&url=").append(curUrl)
//                            .append("&t_ran=").append(PandaProcessor.getRandomStr()).toString());
//                    anchor.setRid(rid);
//                    anchor.setName(names.get(i));
//                    anchor.setTitle(titles.get(i));
//                    anchor.setCategory(categories.get(i));
//                    anchor.setPopularityStr(popularitiyStr);
//                    anchor.setPopularityNum(popularitiyNum);
//                    anchor.setJob(job);
//                    anchor.setPlat(Const.DOUYU);
//                    anchor.setGame(Const.GAMEALL);
//                    anchor.setUrl(curUrl);
//                    new Thread(new Runnable() {
//                        @Override
//                        public void run() {
//                            HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
//                                    .append("&par_d=").append(date).append(anchor.toString()).toString());
//                        }
//                    }).start();
//                    Thread.sleep(5);
//                    anchorObjs.add(anchor);

//                    resultSetStr.add(anchor.toString());
//                }
                String json = page.getJson().get();
                JSONArray list = JsonPath.read(json,"$.data.rl");
                for (int i=0;i<list.size();i++){
                    Anchor anchor = new Anchor();
                    String room = list.get(i).toString();
                    anchor.setRid(JsonPath.read(room,"$.rid").toString());
                    anchor.setName(JsonPath.read(room,"$.nn"));
                    anchor.setTitle(JsonPath.read(room,"$.rn"));
                    anchor.setCategory(JsonPath.read(room,"$.c2name"));
                    anchor.setPopularityNum(JsonPath.read(room,"$.ol"));
                    anchor.setJob(job);
                    anchor.setPlat(Const.DOUYU);
                    anchor.setGame(Const.GAMEALL);
                    anchor.setUrl(curUrl);
                    resultSetStr.add(anchor.toString());
                }
            }
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.error("execute faild,url:" + curUrl);
            e.printStackTrace();
            if (++exCnt % 10 == 0) {
                MailTools.sendAlarmmail("douyuanchor 异常请求个数过多", "url: " + failedUrl.toString());
//                System.exit(1);
            }
        }

    }

    @Override
    public Site getSite() {
        super.getSite();
        return this.site;
    }

    public static void main(String[] args) {
        args = new String[]{"douyuanchor"};
        crawler(args);
    }
}
