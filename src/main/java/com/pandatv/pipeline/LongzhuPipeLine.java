package com.pandatv.pipeline;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.IOTools;
import net.minidev.json.JSONArray;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class LongzhuPipeLine implements Pipeline{
    private static String job;
    private static BufferedWriter bw;

    public LongzhuPipeLine(String job, BufferedWriter bw) {
        this.job=job;
        this.bw=bw;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
        String curUrl = resultItems.getRequest().getUrl();
        String json = resultItems.get("json");
        JSONArray items = JsonPath.read(json, "$.data.items");
        List<String> results = new ArrayList<>();
//        for (int i=0;i<items.size();i++){
//            Anchor anchor = new Anchor();
//            String room = items.get(i).toString();
//            String rid = JsonPath.read(room, "$.channel.domain");
//            String name = JsonPath.read(room, "$.channel.name");
//            String title = JsonPath.read(room, "$.channel.status");
//            String category = JsonPath.read(room, "$.game[0].name");
//            String popularitiyStr = JsonPath.read(room,"$.viewers");
//            int popularitiyNum = Integer.parseInt(popularitiyStr);
//            anchor.setRid(rid);
//            anchor.setName(name);
//            anchor.setTitle(title);
//            anchor.setCategory(category);
//            anchor.setPopularityStr(popularitiyStr);
//            anchor.setPopularityNum(popularitiyNum);
//            anchor.setJob(job);
//            anchor.setPlat(Const.LONGZHU);
//            anchor.setGame(Const.GAMEALL);
//            anchor.setUrl(curUrl);
//            String result = anchor.toString();
//            results.add(result);
//        }
        IOTools.writeList(results, bw);
    }
}
