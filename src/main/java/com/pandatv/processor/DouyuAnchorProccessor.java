package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.HttpUtil;
import com.pandatv.tools.MailTools;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.util.Date;
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
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
//        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        String firstUrl = "https://www.douyu.com/directory/all";
        long start = System.currentTimeMillis();
        Spider.create(new DouyuAnchorProccessor()).addUrl(firstUrl).thread(1).setDownloader(new PandaDownloader()).addPipeline(new ConsolePipeline()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs));
//        for (Anchor anchor : anchorObjs) {
//            anchors.add(anchor.toString());
//        }
//        CommonTools.writeAndMail(hivePaht, Const.DOUYUFINISH, anchors);
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
                    page.addTargetRequest("https://www.douyu.com/directory/all?isAjax=1&page=" + i);
                }
                page.setSkip(true);
            } else {
                List<String> rids = page.getHtml().xpath("//body/li/@data-rid").all();
                List<String> names = page.getHtml().xpath("//body/li/a/div[@class='mes']/p/span[@class='dy-name ellipsis fl']/text()").all();
                List<String> titles = page.getHtml().xpath("//body/li/a/@title").all();
                List<String> popularities = page.getHtml().xpath("//body/li/a/div[@class='mes']/p/span[@class='dy-num fr']/text()").all();
                List<String> categories = page.getHtml().xpath("//body/li/a/div[@class='mes']/div[@class='mes-tit']/span/text()").all();
                for (int i = 0; i < names.size(); i++) {
//                    Anchor anchor = new Anchor();
                    String popularitiyStr = popularities.get(i);
                    int popularitiyNum = CommonTools.createNum(popularitiyStr);
                    String rid = rids.get(i);
                    if (rid.contains("\u0001")) {
                        logger.info("rid contains SEP,url:{},rid:{}", url, rid);
                    }
                    if (!CommonTools.isValidUnicode(rid)) {
                        logger.info("rid is not valid unicode,url:{},rid:{}", url, rid);
                    }
                    HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
                            .append("&par_d=").append(date)
                            .append("&rid=").append(rid)
                            .append("&nm=").append(CommonTools.getFormatStr(names.get(i)))
                            .append("&tt=").append(CommonTools.getFormatStr(titles.get(i)))
                            .append("&cate=").append(categories.get(i))
                            .append("&pop_s=").append(popularitiyStr)
                            .append("&pop_n=").append(popularitiyNum)
                            .append("&task=").append(job)
                            .append("&plat=").append(Const.DOUYU)
                            .append("&url_c=").append(Const.GAMEALL)
                            .append("&c_time=").append(createTimeFormat.format(new Date()))
                            .append("&url=").append(curUrl)
                            .append("&t_ran=").append(PandaProcessor.getRandomStr()).toString());
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
//                    anchorObjs.add(anchor);
                }
            }
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.error("execute faild,url:" + curUrl);
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail("斗鱼首页推荐", "url: " + curUrl);
                System.exit(1);
            }
        }

    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void main(String[] args) {
        args = new String[]{"douyuanchor"};
        crawler(args);
    }
}
