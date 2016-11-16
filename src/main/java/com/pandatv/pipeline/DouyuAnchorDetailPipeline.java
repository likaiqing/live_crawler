package com.pandatv.pipeline;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.BufferedWriter;

/**
 * Created by likaiqing on 2016/11/15.
 */
public class DouyuAnchorDetailPipeline implements Pipeline {
    private static String job;
    private static BufferedWriter bw;
    public DouyuAnchorDetailPipeline(String job, BufferedWriter bw) {
        this.job = job;
        this.bw = bw;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
        String curUrl = resultItems.get("curUrl");
        String rid = curUrl.substring(curUrl.lastIndexOf('/')+1);
    }
}
