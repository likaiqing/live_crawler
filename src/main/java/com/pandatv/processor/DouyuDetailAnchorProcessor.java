package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.pipeline.DouyuDetailAnchorPipeline;
import com.pandatv.pojo.DetailAnchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.IOTools;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.selector.Html;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/15.
 */
public class DouyuDetailAnchorProcessor extends PandaProcessor {
    private static List<String> detailAnchors = new ArrayList<>();
    private static String thirdApi = "http://open.douyucdn.cn/api/RoomApi/room";
    private static String job = "";
    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().toString();

        if (curUrl.equals("https://www.douyu.com/directory/all")) {
            Html html = page.getHtml();
            String js = page.getHtml().getDocument().getElementsByAttributeValue("type", "text/javascript").get(3).toString();
            int endPage = Integer.parseInt(js.substring(js.indexOf("count:") + 8, js.lastIndexOf(',') - 1));
            for (int i = 1; i < endPage; i++) {
                Request request = new Request("https://www.douyu.com/directory/all?isAjax=1&page=" + i).setPriority(1);
                page.addTargetRequest(request);
                page.setSkip(true);
            }
        } else if (curUrl.startsWith("https://www.douyu.com/directory/all?isAjax=1&page=")) {
            Html html = page.getHtml();
            List<String> detailUrls = page.getHtml().xpath("//body/li/a/@href").all();
            for (String url : detailUrls) {
                Request request = new Request(thirdApi+url.substring(url.lastIndexOf("/"))).setPriority(3);
                page.addTargetRequest(request);
                page.setSkip(true);
            }
        } else {
            String json = page.getJson().get();
            int error = JsonPath.read(json, "$.error");
            DetailAnchor detailAnchor = new DetailAnchor();
            String rid = JsonPath.read(json,"$.data.room_id");
            String name = JsonPath.read(json,"$.data.owner_name");
            String title = JsonPath.read(json,"$.data.room_name");
            String categorySec = JsonPath.read(json,"$.data.cate_name");
            int viewerStr  = JsonPath.read(json,"$.data.online");
            String followerStr  = JsonPath.read(json,"$.data.fans_num");
            String weightStr = JsonPath.read(json,"$.data.owner_weight");
            detailAnchor.setRid(rid);
            detailAnchor.setName(name);
            detailAnchor.setTitle(title);
            detailAnchor.setCategorySec(categorySec);
            detailAnchor.setViewerNum(viewerStr);
            detailAnchor.setFollowerNum(Integer.parseInt(followerStr));
            detailAnchor.setWeightNum(CommonTools.getDouyuWeight(weightStr));
            detailAnchor.setUrl(curUrl);
            detailAnchors.add(detailAnchor.toString());
            if (detailAnchors.size()!=Const.WRITEBATCH){
                page.setSkip(true);
            }
//            Html html = page.getHtml();
//            String name = html.xpath("//div[@class='acinfo-fs-con  clearfix']/ul/li[1]/div/a/text()").get();//比较多的
//            String title = null;
//            String categoryFir = null;
//            String categorySec = null;
//            String viewerStr = null;
//            String followerStr = null;
//            String rankStr = null;
//            String weightStr = null;
//            String tag = null;
//            String notice = null;
//            if (!StringUtils.isEmpty(name)) {
//                title = html.xpath("//div[@class='headline clearfix']/h1/text()").get();
//                categoryFir = html.xpath("//div[@class='tag-fs-con clearfix']/dl/dd/a[1]/text()").get();
//                categorySec = html.xpath("//div[@class='tag-fs-con clearfix']/dl/dd/a[2]/text()").get();
//                viewerStr = html.xpath("//div[@class='num-box']/div[@class='num-v-con']/a/text()").get();
//                followerStr = html.xpath("//div[@class='focus-box-con clearfix']/p/span/text()").get();
//                rankStr = html.xpath("//div[@class='catagory-order-num fl']/span[1]/span/text()").get();
//                weightStr = html.xpath("//div[@class='weight-v-con']/a/text()").get();
//                tag = html.xpath("//div[@class='tag-fs-con clearfix']/dl/dd/a/text()").get();
//                notice = html.xpath("//p[@class='column-cotent']/text()").get();
//
//            } else {//有可能是转播的qq直播
//                name = html.xpath("//ul[@class='r-else clearfix']/li[1]/i[@class='zb-name']/text()").get();
//                title = html.xpath("//ul[@class='headline clearfix']/h1/text()").get();
//                viewerStr = html.xpath("//ul[@class='r-else clearfix']/li[2]/span/span/text()").get();
//                tag = html.xpath("//ul[@class='r-else clearfix']/li[3]/a/text()").get();
//                weightStr = html.xpath("//ul[@class='r-else clearfix']/li[4]/span/span/text()").get();
//                followerStr = html.xpath("//div[@class='focus-box']/p/span/text()").get();
//                notice = html.xpath("//div[@class='column o-notice']/div[@class='column-cont']/p/text()").get();
//            }
//            page.putField("name", name);
        }
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        job = args[0];//douyuanchordetail
        String date = args[1];
        String hour = args[2];
        BufferedWriter bw = IOTools.getBW(Const.FILEDIR + job + "_" + date + "_" + hour + ".csv");
        String firstUrl = "https://www.douyu.com/directory/all";
        Spider.create(new DouyuDetailAnchorProcessor()).thread(1).addUrl(firstUrl).addPipeline(new DouyuDetailAnchorPipeline(detailAnchors, bw)).run();//.setDownloader(new SeleniumDownloader(Const.CHROMEDRIVER))
        IOTools.writeList(detailAnchors, bw);
        IOTools.closeBw(bw);
    }
}
