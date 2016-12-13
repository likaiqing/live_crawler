package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pipeline.DouyuNewlivePipeline;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.*;
import net.minidev.json.JSONArray;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class DouyuNewLiveProccessor extends PandaProcessor {
    private static int minPage = 30;
    private static int maxPage = 100;
    private static String url = "https://www.douyu.com/member/recommlist/getfreshlistajax?bzdata=0&clickNum=";
    private static String thirdApi = "http://open.douyucdn.cn/api/RoomApi/room/";
    private static Set<DetailAnchor> detailAnchorSet = new HashSet<>();
    private static int exCnt;

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().toString();
        try {
            if (curUrl.startsWith(url)) {
                String json = page.getJson().toString();
                int curPage = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf('=') + 1));
                JSONArray rooms = JsonPath.read(json, "$.room");
                Set<String> newRoomUrls = new HashSet<>();
                for (int i = 0; i < rooms.size(); i++) {
                    String roomId = JsonPath.read(rooms.get(i).toString(), "$.roomid");
                    DetailAnchor detailAnchor = new DetailAnchor(roomId);
                    if (detailAnchorSet.contains(detailAnchor)) {
                        continue;
                    }
                    newRoomUrls.add(thirdApi + roomId);
                }
                page.addTargetRequests(new ArrayList<String>(newRoomUrls), 5);
                if (curPage < minPage || (curPage < maxPage && newRoomUrls.size() != 0)) {
                    page.putField("json", json);
                    page.addTargetRequest(url + (curPage + 1));
                } else {
                    System.out.println("out");
                }
            } else if (curUrl.startsWith(thirdApi)) {
                Object cycleTriedTimes = page.getRequest().getExtra("_cycle_tried_times");
                if (null != cycleTriedTimes && (int) cycleTriedTimes >= Const.CYCLERETRYTIMES - 1) {
                    timeOutUrl.append(curUrl).append(";");
                }
                String json = page.getJson().get();
                DetailAnchor detailAnchor = new DetailAnchor();
                String rid = JsonPath.read(json, "$.data.room_id");
                String name = JsonPath.read(json, "$.data.owner_name");
                String title = JsonPath.read(json, "$.data.room_name");
                String categorySec = JsonPath.read(json, "$.data.cate_name");
                int viewerStr = JsonPath.read(json, "$.data.online");
                String followerStr = JsonPath.read(json, "$.data.fans_num");
                String weightStr = JsonPath.read(json, "$.data.owner_weight");
                String lastStartTime = JsonPath.read(json, "$.data.start_time");
                detailAnchor.setRid(rid);
                detailAnchor.setName(name);
                detailAnchor.setTitle(title);
                detailAnchor.setCategorySec(categorySec);
                detailAnchor.setViewerNum(viewerStr);
                detailAnchor.setFollowerNum(Integer.parseInt(followerStr));
                detailAnchor.setWeightNum(CommonTools.getDouyuWeight(weightStr));
                detailAnchor.setUrl(curUrl);
                detailAnchor.setLastStartTime(lastStartTime);
                detailAnchor.setJob(job);
                detailAnchorSet.add(detailAnchor);
            }
        } catch (Exception e) {
            failedUrl.append(curUrl + ";");
//            mail.sendAlarmmail(Const.HUYAEXFLAG, "url: " + curUrl);
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail("斗鱼新秀process异常过多退出", "url: " + curUrl);
                System.exit(1);
            }
        }
        page.setSkip(true);
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        job = args[0];
        date = args[1];
        hour = args[2];
        String hivePaht = Const.HIVEDIR + "panda_detail_anchor_crawler/" + date + hour;
        Spider.create(new DouyuNewLiveProccessor()).addUrl(url + "1").thread(1).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();//.setDownloader(new PandaDownloader())
        List<String> anchorList = new ArrayList<>();
        for (DetailAnchor detailAnchor : detailAnchorSet) {
            anchorList.add(detailAnchor.toString());
        }
        CommonTools.writeAndMail(hivePaht, Const.DOUYUNEWLIVEFINISH, anchorList);
    }

    public static void main(String[] args) {
        args = new String[]{"newlive"};
        crawler(args);
    }
}
