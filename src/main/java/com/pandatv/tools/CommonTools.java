package com.pandatv.tools;

import com.pandatv.pipeline.HuyaAnchorPipeline;
import com.pandatv.pojo.Anchor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class CommonTools {
    private static final Logger logger = LoggerFactory.getLogger(CommonTools.class);
    public static List<String> getUrls(ResultItems resultItems, String job, String plat, String gameCategory) {

        List<String> results = new ArrayList<>();
        String url = resultItems.getRequest().getUrl();
        List<String> names = resultItems.get("names");
        List<String> popularities = resultItems.get("popularities");
        List<String> rids = resultItems.get("rids");
        List<String> titles = resultItems.get("titles");
        List<String> categories = resultItems.get("categories");
        for (int i = 0; i < names.size(); i++) {
            Anchor anchor = new Anchor();
            String popularitiyStr = popularities.get(i);
            int popularitiyNum = createNum(popularitiyStr);
            String rid = rids.get(i);
            if (rid.contains("\u0001")){
                logger.info("rid contains SEP,url:{},rid:{}",url,rid);
            }
            anchor.setRid(rid);
            anchor.setName(names.get(i));
            anchor.setTitle(titles.get(i));
            anchor.setCategory(categories.get(i));
            anchor.setPopularityStr(popularitiyStr);
            anchor.setPopularityNum(popularitiyNum);
            anchor.setJob(job);
            anchor.setPlat(plat);
            anchor.setGame(gameCategory);
            anchor.setUrl(url);
            String result = anchor.toString();
            results.add(result);
        }
        return results;
    }

    private static int createNum(String popularitiyStr) {
        if (!popularitiyStr.contains("万")) {
            return Integer.parseInt(popularitiyStr);
        }
        double num = Double.parseDouble(popularitiyStr.substring(0, popularitiyStr.indexOf('万'))) * 10000;
        return (int) num;
    }
    public  static boolean isValidUnicode(String str) {
        for (int i = 0; i < str.length(); i++) {

            int c = str.codePointAt(i);
            if (c < 0x0000 || c > 0xffff) {
                return false;
            }
        }
        return true;
    }
}
