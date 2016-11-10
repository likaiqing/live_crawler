package com.pandatv.pipeline;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.lang.ObjectUtils;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.selector.Json;

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
            JsonPath.read(json,"$.room");
        }catch (Exception e){
            e.printStackTrace();
        }
        //TODO
    }
}
