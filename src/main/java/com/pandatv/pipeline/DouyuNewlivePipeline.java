package com.pandatv.pipeline;


import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.IOTools;
import net.minidev.json.JSONArray;
import org.apache.commons.lang.ObjectUtils;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.selector.Json;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class DouyuNewlivePipeline implements Pipeline {
    private static String job;
    public DouyuNewlivePipeline(){
        super();
    }
    public DouyuNewlivePipeline(String job){
        this.job=job;
    }
    @Override
    public void process(ResultItems resultItems, Task task) {
        String json = resultItems.get("json");
        try {
            JSONArray rooms = JsonPath.read(json, "$.room");
            List<String> urls = new ArrayList<>();
            for (int i=0;i<rooms.size();i++){
                Anchor anchor = new Anchor();
                String room = rooms.get(i).toString();
                String roomId = JsonPath.read(room, "$.roomid");
                String name = JsonPath.read(room, "$.nickname");
                String title = JsonPath.read(room, "$.roomname");
                String category = JsonPath.read(room, "$.gamename");
                anchor.setRid(Integer.parseInt(roomId));

            }
            IOTools.writeList(urls, Const.FILEDIR + job + ".csv");
        }catch (Exception e){
            e.printStackTrace();
        }
        //TODO
    }
}
