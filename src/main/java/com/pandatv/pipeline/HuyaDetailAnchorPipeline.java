package com.pandatv.pipeline;

import com.pandatv.tools.HiveJDBCConnect;
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
    private static HiveJDBCConnect hive;
    private static String hivePaht;
    public HuyaDetailAnchorPipeline(List<String> detailAnchors, HiveJDBCConnect hive, String hivePaht) {
        this.detailAnchors = detailAnchors;
        this.hive = hive;
        this.hivePaht=hivePaht;
    }

    @Override
    public void process(ResultItems resultItems, Task task) {
//        IOTools.writeList(detailAnchors,bw);
//        hive.write2(hivePaht,detailAnchors);
//        this.detailAnchors.clear();
    }
}
