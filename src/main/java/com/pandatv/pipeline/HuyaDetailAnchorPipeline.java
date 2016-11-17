package com.pandatv.pipeline;

import com.pandatv.tools.IOTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.BufferedWriter;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/16.
 */
public class HuyaDetailAnchorPipeline implements Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(HuyaDetailAnchorPipeline.class);
    private static List<String > detailAnchors;
    private static BufferedWriter bw;
    public HuyaDetailAnchorPipeline(List<String> detailAnchors, BufferedWriter bw) {
        this.detailAnchors = detailAnchors;
        this.bw = bw;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
        IOTools.writeList(detailAnchors,bw);
        this.detailAnchors.clear();
    }
}
