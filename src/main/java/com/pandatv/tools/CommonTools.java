package com.pandatv.tools;

import com.pandatv.pojo.Anchor;
import us.codecraft.webmagic.ResultItems;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class CommonTools {
    public static List<String> getUrls(ResultItems resultItems, String job, String plat, String gameCategory) {
        List<String> urls = new ArrayList<>();
        List<String> names = resultItems.get("names");
        List<String> popularities = resultItems.get("popularities");
        List<String> rids = resultItems.get("rids");
        List<String> titles = resultItems.get("titles");
        List<String> categories = resultItems.get("categories");
        for (int i = 0; i < names.size(); i++) {
            Anchor anchor = new Anchor();
            String popularitiyStr = popularities.get(i);
            int popularitiyNum = createNum(popularitiyStr);
            anchor.setRid(rids.get(i));
            anchor.setName(names.get(i));
            anchor.setTitle(titles.get(i));
            anchor.setCategory(categories.get(i));
            anchor.setPopularityStr(popularitiyStr);
            anchor.setPopularityNum(popularitiyNum);
            anchor.setJob(job);
            anchor.setPlat(plat);
            anchor.setGame(gameCategory);
            String result = anchor.toString();
            urls.add(result);
        }
        return urls;
    }

    private static int createNum(String popularitiyStr) {
        if (!popularitiyStr.contains("万")) {
            return Integer.parseInt(popularitiyStr);
        }
        double num = Double.parseDouble(popularitiyStr.substring(0, popularitiyStr.indexOf('万'))) * 10000;
        return (int) num;
    }
}
