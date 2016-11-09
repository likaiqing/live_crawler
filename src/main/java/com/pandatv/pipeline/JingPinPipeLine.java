package com.pandatv.pipeline;

import com.pandatv.common.Const;
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
    private static String hour;

    public JingPinPipeLine() {
        super();
    }

    public JingPinPipeLine(String gameCategory, String job, String plat, String hour) {
        this.gameCategory = gameCategory;
        this.job = job;
        this.plat = plat;
        this.hour = hour;
    }

//    public DouyuePipeLine(BufferedWriter bw) {
//        this.bw=bw;
//    }

    public void process(ResultItems resultItems, Task task) {
        List<String> urls = new ArrayList<>();
        List<String> names = resultItems.get("names");
        List<String> popularities = resultItems.get("popularities");
        List<String> rids = resultItems.get("rids");
        List<String> titles = resultItems.get("titles");
        List<String> categories = resultItems.get("categories");
        for (int i = 0; i < names.size(); i++) {
            String popularitiyStr = popularities.get(i);
            int popularitiyNum = createNum(popularitiyStr);
            String result = new StringBuffer(rids.get(i)).append(Const.SEP).append(names.get(i)).append(Const.SEP).append(titles.get(i).trim()).append(Const.SEP).append(categories.get(i)).append(Const.SEP).append(popularitiyStr).append(Const.SEP).append(popularitiyNum).append(Const.SEP).append(job).append(Const.SEP).append(plat).append(Const.SEP).append(gameCategory).toString();
            urls.add(result);
        }
        IOTools.writeList(urls, Const.FILEDIR + job + "_" + plat + "_" + gameCategory + "_" + hour + ".csv");
    }

    private int createNum(String popularitiyStr) {
        if (!popularitiyStr.contains("万")) {
            return Integer.parseInt(popularitiyStr);
        }
        double num = Double.parseDouble(popularitiyStr.substring(0, popularitiyStr.indexOf('万'))) * 10000;
        return (int) num;
    }

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {
        int num = JingPinPipeLine.class.newInstance().createNum("1213");
        System.out.printf(num + "");
    }
}
