package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.CommonTools;
import net.minidev.json.JSONArray;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

/**
 * Created by likaiqing on 2016/12/14.
 */
public class PandaAnchorProcessor extends PandaProcessor {
    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        try {
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
                    Anchor anchor = new Anchor();
                    anchor.setRid(rid);
                    anchor.setName(name);
                    anchor.setTitle(title);
                    anchor.setCategory(category);
                    anchor.setPopularityStr(popularitiyStr);
                    anchor.setPopularityNum(popularitiyNum);
                    anchor.setJob(job);
                    anchor.setPlat(Const.PANDA);
                    anchor.setGame(Const.GAMEALL);
                    anchor.setUrl(curUrl);
                    anchorObjs.add(anchor);
                }
            }
            page.setSkip(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        String firUrl = "http://www.panda.tv/live_lists?status=2&order=person_num&pagenum=120&pageno=1";
        job = args[0];//pandaanchor
        date = args[1];//20161114
        hour = args[2];//10
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        Spider.create(new PandaAnchorProcessor()).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        for (Anchor anchor : anchorObjs) {
            anchors.add(anchor.toString());
        }
        CommonTools.writeAndMail(hivePaht, Const.PANDAANCHORFINISH, anchors);
    }
}
