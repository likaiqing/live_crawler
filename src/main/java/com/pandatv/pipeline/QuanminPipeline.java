package com.pandatv.pipeline;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.IOTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class QuanminPipeline implements Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(QuanminPipeline.class);
    private static String job;
    private static BufferedWriter bw;
    public QuanminPipeline(String job, BufferedWriter bw) {
        this.job = job;
        this.bw = bw;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
//        String curUrl = resultItems.getRequest().getUrl();
//        String json = resultItems.get("json");
//        JSONArray data = JsonPath.read(json, "$.data");
//        List<String> results = new ArrayList<>();
//        for (int i=0;i<data.size();i++){
//            Anchor anchor = new Anchor();
//            String  room = data.get(i).toString();
//            String rid = JsonPath.read(room,"$.uid");
//            String name = JsonPath.read(room,"$.nick");
//            String title = JsonPath.read(room,"$.title");
//            String category = JsonPath.read(room,"$.category_name");
//            String popularityStr = JsonPath.read(room,"$.view");
//            int popularityNum = Integer.parseInt(popularityStr);
//            anchor.setRid(rid);
//            anchor.setName(name);
//            anchor.setTitle(title);
//            anchor.setCategory(category);
//            anchor.setPopularityStr(popularityStr);
//            anchor.setPopularityNum(popularityNum);
//            anchor.setJob(job);
//            anchor.setPlat(Const.QUANMIN);
//            anchor.setGame(Const.GAMEALL);
//            anchor.setUrl(curUrl);
//            String result = anchor.toString();
//            results.add(result);
//        }
//        IOTools.writeList(results, bw);
    }
}
