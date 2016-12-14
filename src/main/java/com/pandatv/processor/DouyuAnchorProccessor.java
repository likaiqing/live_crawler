package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.CommonTools;
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

    public static void crawler(String[] args) {
        job = args[0];//douyuanchor
        date = args[1];
        hour = args[2];
        String hivePaht = Const.HIVEDIR + "panda_anchor_crawler/" + date + hour;
        String firstUrl = "https://www.douyu.com/directory/all";
        Spider.create(new DouyuAnchorProccessor()).addUrl(firstUrl).thread(1).setDownloader(new PandaDownloader()).addPipeline(new ConsolePipeline()).run();
        CommonTools.writeAndMail(hivePaht, Const.DOUYUFINISH, anchors);
    }

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().toString();
        logger.info("process url:{}", curUrl);
        if (curUrl.equals("https://www.douyu.com/directory/all")) {
            String js = page.getHtml().getDocument().getElementsByAttributeValue("type", "text/javascript").get(3).toString();
            int endPage = Integer.parseInt(js.substring(js.indexOf("count:") + 8, js.lastIndexOf(',') - 1));
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
                Anchor anchor = new Anchor();
                String popularitiyStr = popularities.get(i);
                int popularitiyNum = CommonTools.createNum(popularitiyStr);
                String rid = rids.get(i);
                if (rid.contains("\u0001")) {
                    logger.info("rid contains SEP,url:{},rid:{}", url, rid);
                }
                if (!CommonTools.isValidUnicode(rid)) {
                    logger.info("rid is not valid unicode,url:{},rid:{}", url, rid);
                }
                anchor.setRid(rid);
                anchor.setName(names.get(i));
                anchor.setTitle(titles.get(i));
                anchor.setCategory(categories.get(i));
                anchor.setPopularityStr(popularitiyStr);
                anchor.setPopularityNum(popularitiyNum);
                anchor.setJob(job);
                anchor.setPlat(Const.DOUYU);
                anchor.setGame(Const.GAMEALL);
                anchor.setUrl(url);
                String result = anchor.toString();
                anchors.add(result);
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
