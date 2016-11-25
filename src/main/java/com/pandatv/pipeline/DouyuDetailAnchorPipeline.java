package com.pandatv.pipeline;

import com.pandatv.tools.HiveJDBCConnect;
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
    private static HiveJDBCConnect hive;
    private static String hivePaht;
    public DouyuDetailAnchorPipeline(List<String> detailAnchors, HiveJDBCConnect hive,String hivePaht) {
        this.detailAnchors = detailAnchors;
        this.hive = hive;
        this.hivePaht=hivePaht;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
//        IOTools.writeList(detailAnchors,bw);
//        hive.write2(hivePaht,detailAnchors);
        this.detailAnchors.clear();
    }
}
