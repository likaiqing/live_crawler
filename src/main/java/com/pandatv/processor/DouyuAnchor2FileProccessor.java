package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.DateTools;
import com.pandatv.tools.IOTools;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.io.BufferedWriter;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class DouyuAnchor2FileProccessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DouyuAnchor2FileProccessor.class);
    private static String listPre = "https://www.douyu.com/gapi/rkc/directory/0_0/";
    private static int exCnt;
    public static void crawler(String[] args) {
        String destFile = args[1];
        String firstUrl = "https://www.douyu.com/directory/all";
        Spider.create(new DouyuAnchor2FileProccessor()).addUrl(firstUrl).thread(1).setDownloader(new PandaDownloader()).addPipeline(new ConsolePipeline()).run();
        List<String> rids = new ArrayList<>();
        String pre = "room.url.";
        String suf = "=https://www.douyu.com/";
        for (String rid : anchors) {
            rids.add(pre + rid + suf + rid);
        }
        IOTools.writeList(rids,destFile);
        if (rids.size()<10){
            MailTools.sendTaskMail("斗鱼抓取主播数少10,time:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()), PandaProcessor.from + "<-->" + DateTools.getCurDate(), (System.currentTimeMillis() - PandaProcessor.s) + "毫秒;", rids.size(), PandaProcessor.timeOutUrl, PandaProcessor.failedUrl);
        }

    }

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().toString();
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
                page.addTargetRequest(listPre + i);
            }
            page.setSkip(true);
        } else {
//            List<String> rids = page.getHtml().xpath("//body/li/@data-rid").all();
//            anchors.addAll(rids);

            String json = page.getJson().get();
            JSONArray list = JsonPath.read(json,"$.data.rl");
            for (int i=0;i<list.size();i++) {
                String room = list.get(i).toString();
                anchors.add(JsonPath.read(room,"$.rid").toString());
            }
            page.setSkip(true);
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
