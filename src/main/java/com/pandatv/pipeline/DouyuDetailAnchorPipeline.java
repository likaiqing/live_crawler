package com.pandatv.pipeline;

import com.pandatv.tools.IOTools;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.BufferedWriter;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/15.
 */
public class DouyuDetailAnchorPipeline implements Pipeline {
    private static List<String > detailAnchors;
    private static BufferedWriter bw;
    public DouyuDetailAnchorPipeline(List<String> detailAnchors, BufferedWriter bw) {
        this.detailAnchors = detailAnchors;
        this.bw = bw;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
        IOTools.writeList(detailAnchors,bw);
        this.detailAnchors.clear();
    }
}
