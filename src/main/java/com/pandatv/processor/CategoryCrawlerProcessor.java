package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.tools.HttpUtil;
import com.pandatv.tools.MailTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import javax.xml.bind.DatatypeConverter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by likaiqing on 2016/12/13.
 */
public class CategoryCrawlerProcessor extends PandaProcessor {
    private static Set<String> categories = new HashSet<>();
    private static final Logger logger = LoggerFactory.getLogger(CategoryCrawlerProcessor.class);
    private static int exCnt;
    private static String chuchouCate = "https://chushou.tv/gamezone/all-areas.htm";
    private static String douyuCate = "https://www.douyu.com/directory";
    private static String huyaCate = "http://www.huya.com/g?areafib=1";
    private static String pandaCate = "http://www.panda.tv/cate";
    private static String longzhuCate = "http://longzhu.com/games/?from=rmallgames";
    private static String zhanqiCate = "https://www.zhanqi.tv/games";
    //    private static String quanminCate = "http://www.quanmin.tv/json/categories/list.json?_t=";
    private static String quanminCate = "http://www.quanmin.tv/category";

    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().get();
        logger.info("process url:{}", curUrl);
        try {
            Html html = page.getHtml();
            if (curUrl.equals(douyuCate)) {
                List<String> urls = html.xpath("//ul[@id='live-list-contentbox']/li/a/@href").all();
                List<String> names = html.xpath("//ul[@id='live-list-contentbox']/li/a/p/text()").all();
                add2Categories(curUrl, urls, names, PlatIdEnum.DOUYU);
            } else if (curUrl.equals(huyaCate)) {
                List<String> urls = html.xpath("//ul[@id='js-game-list']/li/a/@href").all();
                List<String> names = html.xpath("//ul[@id='js-game-list']/li/a/h3/text()").all();
                add2Categories(curUrl, urls, names, PlatIdEnum.HUYA);
            } else if (curUrl.equals(zhanqiCate)) {
                List<String> urls = html.xpath("//ul[@id='game-list-panel']/li/a/@href").all();
                List<String> names = html.xpath("//ul[@id='game-list-panel']/li/a/p/text()").all();
                add2Categories(curUrl, urls, names, PlatIdEnum.ZHANQI);
            } else if (curUrl.startsWith(quanminCate)) {
//                JSONArray jsonArray = JsonPath.read(page.getJson().get(), "$");
//                for (Object obj : jsonArray) {
//                    String eName = JsonPath.read(obj, "$.slug");
//                    String cName = JsonPath.read(obj, "$.name");
//                    String url = "http://www.quanmin.tv/game/" + eName;
//                StringBuffer sb = new StringBuffer(PlatIdEnum.QUANMIN.platId);
//                sb.append(Const.SEP).append(PlatIdEnum.QUANMIN.paltName).append(Const.SEP).append(eName).append(Const.SEP).append(cName).append(Const.SEP).append(url).append(Const.SEP).append(curUrl).append(Const.SEP).append(getRandomStr());
//                    categories.add(sb.toString());
//                }
                List<String> enameUrls = html.xpath("//div[@class='list_w-card_wrap']/a/@href").all();
                List<String> cnames = html.xpath("//div[@class='list_w-card_wrap']/a/@title").all();
                add2Categories(curUrl, enameUrls, cnames, PlatIdEnum.QUANMIN);
            } else if (curUrl.equals(longzhuCate)) {
                List<String> urls = html.xpath("//div[@class='list-con']/div[@class='list-item']/h2/a/@href").all();
                List<String> names = html.xpath("//div[@class='list-con']/div[@class='list-item']/h2/a/text()").all();
                add2Categories(curUrl, urls, names, PlatIdEnum.LONGZHU);
            } else if (curUrl.equals(chuchouCate)) {
                List<String> urls = html.xpath("//div[@class='gamezone-areas-con']/div[@class='zone-item']/div[@class='zone-item-con']/a/@href").all();
                List<String> names = html.xpath("//div[@class='gamezone-areas-con']/div[@class='zone-item']/div[@class='zone-item-con']/span/text()").all();
                add2Categories(curUrl, urls, names, PlatIdEnum.CHUSHOU);
            } else if (curUrl.equals(pandaCate)) {
                List<String> urls = html.xpath("//ul[@class='sort-menu video-list clearfix']/li/a/@href").all();
                List<String> names = html.xpath("//ul[@class='sort-menu video-list clearfix']/li/a/div[@class='cate-title']/text()").all();
                add2Categories(curUrl, urls, names, PlatIdEnum.PANDA);
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

    private void add2Categories(String curUrl, List<String> urls, List<String> names, PlatIdEnum platIdEnum) {
        for (int i = 0; i < urls.size(); i++) {
            StringBuffer sb = new StringBuffer();
            String url = urls.get(i);
            String name = names.get(i);
            HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.CATEGORYEVENT)
                    .append("&par_d=").append(date)
                    .append("&p_id=").append(platIdEnum.platId)
                    .append("&p_nm=").append(platIdEnum.paltName)
                    .append("&e_n=").append(url.substring(url.lastIndexOf("/") + 1))
                    .append("&c_n=").append(DatatypeConverter.printBase64Binary(name.getBytes()))
                    .append("&url=").append(url)
                    .append("&ent_url=").append(curUrl)
                    .append("&t_ran=").append(DatatypeConverter.printBase64Binary(getRandomStr().getBytes())).toString());
//            sb.append(platIdEnum.platId).append(Const.SEP).append(platIdEnum.paltName).append(Const.SEP).append(url.substring(url.lastIndexOf("/") + 1)).append(Const.SEP).append(name).append(Const.SEP).append(url).append(Const.SEP).append(curUrl).append(Const.SEP).append(getRandomStr());
//            categories.add(sb.toString());
        }
    }

    @Override
    public Site getSite() {
        return this.site.setHttpProxy(null);
    }

    public static void crawler(String[] args) {
        job = args[0];//categorycrawler
        date = args[1];//20161114
        hour = args[2];//
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_category/" + date;
        long start = System.currentTimeMillis();
        //douyuCate, huyaCate, chuchouCate, zhanqiCate, longzhuCate, pandaCate, quanminCate /**+ new SimpleDateFormat("yyyyMMddHHmm").format(new Date())*/
        Spider.create(new CategoryCrawlerProcessor()).addUrl(huyaCate).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs));
//        CommonTools.writeAndMail(hivePaht, Const.CATEGORYFINISH, categories);
    }

    public static enum PlatIdEnum {
        PANDA(1, "panda"),
        DOUYU(2, "douyu"),
        HUYA(3, "huya"),
        ZHANQI(4, "zhanqi"),
        QUANMIN(5, "quanmin"),
        LONGZHU(6, "longzhu"),
        CHUSHOU(7, "chushou");

        private int platId;
        private String paltName;

        private PlatIdEnum(int platId, String paltName) {
            this.platId = platId;
            this.paltName = paltName;
        }
    }
}
