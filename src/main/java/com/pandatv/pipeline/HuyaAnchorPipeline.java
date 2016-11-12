package com.pandatv.pipeline;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.IOTools;
import net.minidev.json.JSONArray;
import org.apache.commons.lang.StringUtils;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/11.
 */
public class HuyaAnchorPipeline implements Pipeline {
    private static String job;
    private static BufferedWriter bw;

    public HuyaAnchorPipeline(String job, BufferedWriter bw) {
        this.job = job;
        this.bw = bw;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
        String json = resultItems.get("json").toString();
        JSONArray list = JsonPath.read(json, "$.data.list");
        List<String> results = new ArrayList<>();
        String url = resultItems.getRequest().getUrl();
        for (int i = 0; i < list.size(); i++) {
            String jsonStr = list.get(i).toString();
            String rid = JsonPath.read(jsonStr, "$.privateHost");
            String name = JsonPath.read(jsonStr, "$.nick");
            String title = JsonPath.read(jsonStr, "$.introduction");
            String category = JsonPath.read(jsonStr, "$.gameFullName");
            String popularityStr = JsonPath.read(jsonStr, "$.totalCount");
            int popularityNum = Integer.parseInt(popularityStr);
            if (StringUtils.isEmpty(rid) || StringUtils.isEmpty(name) || StringUtils.isEmpty(title) || StringUtils.isEmpty(category) || StringUtils.isEmpty(popularityStr)) {
                continue;
            }
            Anchor anchor = new Anchor();
            anchor.setRid(rid);
            anchor.setName(name);
            anchor.setTitle(title);
            anchor.setCategory(category);
            anchor.setPopularityStr(popularityStr);
            anchor.setPopularityNum(popularityNum);
            anchor.setJob(job);
            anchor.setPlat("huya");
            anchor.setGame("all");
            anchor.setUrl(url);
            results.add(anchor.toString());
        }
        IOTools.writeList(results, bw);
    }
}
