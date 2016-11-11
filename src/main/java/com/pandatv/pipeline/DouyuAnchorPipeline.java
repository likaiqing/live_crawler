package com.pandatv.pipeline;

import com.pandatv.common.Const;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.IOTools;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.BufferedWriter;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class DouyuAnchorPipeline implements Pipeline {
    private static String job;
    private static BufferedWriter bw;

    public DouyuAnchorPipeline() {
        super();
    }

    public DouyuAnchorPipeline(String job,BufferedWriter bw) {
        this.job = job;
        this.bw = bw;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
        List<String> urls = CommonTools.getUrls(resultItems, job, Const.DOUYU, Const.GAMEALL);
        IOTools.writeList(urls, bw);
    }
}
