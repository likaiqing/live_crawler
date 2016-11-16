package com.pandatv.pipeline;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.CommonTools;
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
public class ChushouPipeline implements Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(ChushouPipeline.class);
    private static String job;
    private static BufferedWriter bw;
    public ChushouPipeline(String job, BufferedWriter bw) {
        this.job = job;
        this.bw = bw;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
        String curUrl = resultItems.getRequest().getUrl();
        resultItems.get("json");
        List<String> results = new ArrayList<>();
        if (!curUrl.equals("http://chushou.tv/live/list.htm")) {
            parseJson(resultItems,results);
        }else {
            parseHtml(resultItems,results);
        }
        IOTools.writeList(results, bw);
    }

    private void parseHtml(ResultItems resultItems, List<String> results) {
        String curUrl = resultItems.getRequest().getUrl();
        List<String> rids = resultItems.get("rids");
        List<String> names = resultItems.get("names");
        List<String> titles = resultItems.get("titles");
        List<String> categories = resultItems.get("categories");
        List<String> popularitiyStrs = resultItems.get("popularitiyStrs");
        for (int i=0;i<rids.size();i++){
            Anchor anchor = new Anchor();
            String rid = rids.get(i);
            anchor.setRid(rid.substring(rid.lastIndexOf("/")+1,rid.lastIndexOf(".")));
            anchor.setName(names.get(i));
            anchor.setTitle(titles.get(i));
            anchor.setCategory(categories.get(i));
            anchor.setPopularityStr(popularitiyStrs.get(i));
            anchor.setPopularityNum(CommonTools.createNum(popularitiyStrs.get(i)));
            anchor.setJob(job);
            anchor.setPlat(Const.CHUSHOU);
            anchor.setGame(Const.GAMEALL);
            anchor.setUrl(curUrl);
            results.add(anchor.toString());
        }
    }

    private void parseJson(ResultItems resultItems, List<String> results) {
        String curUrl = resultItems.getRequest().getUrl();
        String json = resultItems.get("json");
        JSONArray read = JsonPath.read(json, "$.data.items");
        for (int i=0;i<read.size();i++){
            String room = read.get(i).toString();
            Anchor anchor = new Anchor();
            anchor.setRid(JsonPath.read(room,"$.targetKey").toString());
            anchor.setName(JsonPath.read(room,"$.meta.creator").toString());
            anchor.setTitle(JsonPath.read(room,"$.name").toString());
            anchor.setCategory(JsonPath.read(room,"$.meta.gameName").toString());
            String popularyStr = JsonPath.read(room,"$.meta.onlineCount").toString();
            anchor.setPopularityStr(popularyStr);
            anchor.setPopularityNum(CommonTools.createNum(popularyStr));
            anchor.setJob(job);
            anchor.setPlat(Const.CHUSHOU);
            anchor.setGame(Const.GAMEALL);
            anchor.setUrl(curUrl);
            results.add(anchor.toString());
        }
    }
}
