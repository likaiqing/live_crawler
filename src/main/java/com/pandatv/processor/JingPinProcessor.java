package com.pandatv.processor;

import com.pandatv.common.PandaProcessor;
import com.pandatv.pipeline.JingPinPipeLine;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;

import java.util.List;

/**
 * Created by likaiqing on 2016/11/7.
 */
public class JingPinProcessor extends PandaProcessor {
    private static String task;
    private static String plat;
    private static String gameCategory;
    //    private static BufferedWriter bw;
    private static List<String> urls;

    public void process(Page page) {
//        String url = page.getUrl().toString();
        List<String> names = null;
        List<String> popularities = null;
        List<String> rids = null;
        List<String> titles = null;
        List<String> categories = null;
        if (plat.equals("douyu")) {
            rids = page.getHtml().xpath("//ul[@class='clearfix play-list']/li/@data-rid").all();
            names = page.getHtml().xpath("//ul[@class='clearfix play-list']/li/a/div[@class='mes']/p/span[@class='dy-name ellipsis fl']/text()").all();
            popularities = page.getHtml().xpath("//ul[@class='clearfix play-list']/li/a/div[@class='mes']/p/span[@class='dy-num fr']/text()").all();
            titles = page.getHtml().xpath("//ul[@class='clearfix play-list']/li/a/div[@class='mes']/div[@class='mes-tit']/h3/text()").all();
            categories = page.getHtml().xpath("//ul[@class='clearfix play-list']/li/a/div[@class='mes']/div[@class='mes-tit']/span/text()").all();
        }
//        List<String> all = page.getHtml().xpath(//ul[@class='clearfix play-list'])
        page.putField("names", names);
        page.putField("popularities", popularities);
        page.putField("rids", rids);
        page.putField("titles", titles);
        page.putField("categories", categories);
//        for (int i=0;i<names.size();i++){
//            String result = new StringBuffer(task).append(Const.SEP).append(gameCategory).append(Const.SEP).append(rids.get(i)).append(Const.SEP).append(names.get(i)).append(Const.SEP).append(titles.get(i).trim()).append(Const.SEP).append(categories.get(i)).append(Const.SEP).append(popularities.get(i)).toString();
//            urls.add(result);
//        }
//        IOTools.writeList(urls,Const.FILEDIR + task + "/" + gameCategory);
    }

    public Site getSite() {
        return site;
    }

    public static void crawler(String[] args) {
        task = args[0];//jingpin
        plat = args[1];//douyu
        gameCategory = args[2];//lol
        String hour = args[3];
//        BufferedWriter bw = IOTools.getBW(Const.FILEDIR + task + "/" + gameCategory);
        String url = "https://www.douyu.com/directory/game/" + gameCategory;
        if (plat.equals("")){
            url="";//新的平台更换新的url
        }
        Spider.create(new JingPinProcessor()).addUrl(url).thread(1).addPipeline(new JingPinPipeLine(gameCategory, task, plat,hour)).run();
    }
}
