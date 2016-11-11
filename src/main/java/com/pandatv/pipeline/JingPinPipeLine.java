package com.pandatv.pipeline;

import com.pandatv.common.Const;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.IOTools;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/7.
 */
public class JingPinPipeLine implements Pipeline {
    //    private static BufferedWriter bw;
    private static String job;
    private static String gameCategory;
    private static String plat;
    private static String date;
    private static String hour;

    public JingPinPipeLine() {
        super();
    }

    public JingPinPipeLine(String gameCategory, String job, String plat, String date, String hour) {
        this.gameCategory = gameCategory;
        this.job = job;
        this.plat = plat;
        this.date = date;
        this.hour = hour;
    }

//    public DouyuePipeLine(BufferedWriter bw) {
//        this.bw=bw;
//    }

    public void process(ResultItems resultItems, Task task) {
        List<String> urls = new ArrayList<>();
        urls = CommonTools.getUrls(resultItems, job, plat, gameCategory);
        IOTools.writeList(urls, Const.FILEDIR + job + "_" + plat + "_" + date + "_" + hour + "_" + gameCategory + ".csv");
    }

}
