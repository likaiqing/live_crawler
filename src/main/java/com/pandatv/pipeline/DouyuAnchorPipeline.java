package com.pandatv.pipeline;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class DouyuAnchorPipeline implements Pipeline {
    private static String task;
    public DouyuAnchorPipeline(){
        super();
    }
    public DouyuAnchorPipeline(String task){
        this.task=task;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {

    }
}
