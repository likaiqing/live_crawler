package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.CommonTools;
import net.minidev.json.JSONArray;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.text.SimpleDateFormat;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class LongzhuAnchorProcessor extends PandaProcessor {
    private static String urlTmp = "http://api.plu.cn/tga/streams?max-results=18&sort-by=views&filter=0&game=0&callback=_callbacks_._36bxu1&start-index=";
    private static int pageCount = 18;
    private static String detailJob = "longzhudetailanchor";
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        String body = page.getHtml().xpath("//body/text()").toString();
        String json = body.substring(body.indexOf('(') + 1, body.lastIndexOf(')'));
        Integer total = JsonPath.read(json, "$.data.totalItems");
        int index = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf('=') + 1));
        if (index < total) {
            page.addTargetRequest(urlTmp + (index + pageCount));
        }
        JSONArray items = JsonPath.read(json, "$.data.items");
        for (int i = 0; i < items.size(); i++) {
            Anchor anchor = new Anchor();
            String room = items.get(i).toString();
            String rid = JsonPath.read(room, "$.channel.domain");
            String name = JsonPath.read(room, "$.channel.name");
            String title = JsonPath.read(room, "$.channel.status");
            String category = JsonPath.read(room, "$.game[0].name");
            String popularitiyStr = JsonPath.read(room, "$.viewers");
            int popularitiyNum = Integer.parseInt(popularitiyStr);
            anchor.setRid(rid);
            anchor.setName(name);
            anchor.setTitle(title);
            anchor.setCategory(category);
            anchor.setPopularityStr(popularitiyStr);
            anchor.setPopularityNum(popularitiyNum);
            anchor.setJob(job);
            anchor.setPlat(Const.LONGZHU);
            anchor.setGame(Const.GAMEALL);
            anchor.setUrl(curUrl);
            DetailAnchor detailAnchor = new DetailAnchor();
            detailAnchor.setRid(rid);
            detailAnchor.setName(name);
            detailAnchor.setTitle(title);
            detailAnchor.setCategoryFir(category);
            detailAnchor.setCategorySec(category);
            detailAnchor.setViewerNum(popularitiyNum);
            detailAnchor.setFollowerNum((Integer) JsonPath.read(room, "$.channel.followers"));
            detailAnchor.setWeightNum((Integer) JsonPath.read(room, "$.channel.flowers"));
            detailAnchor.setLastStartTime(getLastStartTime((Long) JsonPath.read(room, "$.channel.broadcast_begin")));//broadcast_begin
            detailAnchor.setJob(detailJob);
            detailAnchor.setUrl(curUrl);
            detailAnchorObjs.add(detailAnchor);
            anchorObjs.add(anchor);
        }
        page.setSkip(true);
    }

    private String getLastStartTime(long broadcastBegin) {
        return format.format(broadcastBegin);
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "http://api.plu.cn/tga/streams?max-results=18&sort-by=views&filter=0&game=0&callback=_callbacks_._36bxu1&start-index=0";
        job = args[0];//longzhuanchor
        date = args[1];//20161114
        hour = args[2];//10
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        Spider.create(new LongzhuAnchorProcessor()).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        for (Anchor anchor : anchorObjs) {
            anchors.add(anchor.toString());
        }
//        CommonTools.writeAndMail(hivePaht, Const.LONGZHUFINISH, anchors);
        job = detailJob;
        for (DetailAnchor detailAnchor : detailAnchorObjs) {
            detailAnchors.add(detailAnchor.toString());
        }
        String hiveDetailPaht = Const.COMPETITORDIR + "crawler_detail_anchor/" + date;
        CommonTools.writeAndMail(hiveDetailPaht, Const.LONGZHUFINISHDETAIL, detailAnchors);
    }
}
