package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.selector.Html;

import java.util.List;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class ChushouAnchorProcessor extends PandaProcessor {
    private static String urlTmp = "http://chushou.tv/live/down-v2.htm?&breakpoint=";
    private static final Logger logger = LoggerFactory.getLogger(ChushouAnchorProcessor.class);
    private static int exCnt;

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        try {
            if (!curUrl.equals("http://chushou.tv/live/list.htm")) {
                String json = page.getJson().toString();
                JSONArray items = JsonPath.read(json, "$.data.items");
                String breakpoint = JsonPath.read(json, "$.data.breakpoint");
                if (items.size() > 0) {
                    page.addTargetRequest(urlTmp + breakpoint);
                    JSONArray read = JsonPath.read(json, "$.data.items");
                    for (int i = 0; i < read.size(); i++) {
                        String room = read.get(i).toString();
                        Anchor anchor = new Anchor();
                        anchor.setRid(JsonPath.read(room, "$.targetKey").toString());
                        anchor.setName(JsonPath.read(room, "$.meta.creator").toString());
                        anchor.setTitle(JsonPath.read(room, "$.name").toString());
                        anchor.setCategory(JsonPath.read(room, "$.meta.gameName").toString());
                        String popularyStr = JsonPath.read(room, "$.meta.onlineCount").toString();
                        anchor.setPopularityStr(popularyStr);
                        anchor.setPopularityNum(CommonTools.createNum(popularyStr));
                        anchor.setJob(job);
                        anchor.setPlat(Const.CHUSHOU);
                        anchor.setGame(Const.GAMEALL);
                        anchor.setUrl(curUrl);
                        anchorObjs.add(anchor);
                    }
                } else {
                    page.setSkip(true);
                }
            } else {
                String breakPoint = page.getHtml().xpath("//div[@class='more']/@data-break").toString();
                page.addTargetRequest(urlTmp + breakPoint);
                Html html = page.getHtml();
                List<String> rids = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='home_live_block']/a/@href").all();
                List<String> titles = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/a/text()").all();
                List<String> names = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='liveDetail']/span[@class='livePlayerName]/text()").all();
                List<String> categories = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='liveDetail']/a[@class='game_Name]/text()").all();
                List<String> popularitiyStrs = html.xpath("//div[@class='block_content']/div[@class='liveCon']/div[@class='liveOne']/div[@class='liveDetail']/span[@class='liveCount]/text()").all();
                for (int i = 0; i < rids.size(); i++) {
                    Anchor anchor = new Anchor();
                    String rid = rids.get(i);
                    anchor.setRid(rid.substring(rid.lastIndexOf("/") + 1, rid.lastIndexOf(".")));
                    anchor.setName(names.get(i));
                    anchor.setTitle(titles.get(i));
                    anchor.setCategory(categories.get(i));
                    anchor.setPopularityStr(popularitiyStrs.get(i));
                    anchor.setPopularityNum(CommonTools.createNum(popularitiyStrs.get(i)));
                    anchor.setJob(job);
                    anchor.setPlat(Const.CHUSHOU);
                    anchor.setGame(Const.GAMEALL);
                    anchor.setUrl(curUrl);
                    anchorObjs.add(anchor);
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

    public static void crawler(String[] args) {
        String firUrl = "http://chushou.tv/live/list.htm";
        job = args[0];//chushouanchor
        date = args[1];//20161114
        hour = args[2];//10
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        Spider.create(new ChushouAnchorProcessor()).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        for (Anchor anchor : anchorObjs) {
            anchors.add(anchor.toString());
        }
        CommonTools.writeAndMail(hivePaht, Const.CHUSHOUFINISH, anchors);
    }
}
