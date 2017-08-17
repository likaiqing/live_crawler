package com.pandatv.processor;

import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.pojo.AnJuKeLouPan;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.MailTools;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.selector.Html;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by likaiqing on 2017/8/17.
 */
public class AnJuKeProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AnJuKeProcessor.class);
    private static final String firUrl = "https://qd.fang.anjuke.com/";
    private static final String firPageUrlTmp = "/?from=";
    private static final String afterFirPageUrlTmp = "/loupan/all/p";
    private static final String detailUrlTmp = ".html?from=AF_RANK_";
    private static final String detailUrlAddTmp = "?from=";
    private static final String cityKeyParam = "city";
    private static final String indexKeyParam = "index";
    private static final String pageKeyParam = "page";
    private static final String sojPre = "AF_RANK_";
    private static Set<String> anjukeList = new HashSet<>();
    private static int exCnt;


    public static void crawler(String[] args) {
        job = args[0];//chushouanchor
        date = args[1];//20161114
        hour = args[2];
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("writeSuccess:" + writeSuccess);
                if (!writeSuccess) {
                    executeMapResults();
                }
            }
        }));
        long start = System.currentTimeMillis();
        Spider.create(new AnJuKeProcessor()).thread(1).addUrl(firUrl).start();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
        executeMapResults();
    }

    private static void executeMapResults() {
        String dirFile = new StringBuffer(Const.CRAWLER_DATA_DIR).append(date).append("/").append(hour).append("/").append(job).append("_").append(date).append("_").append(hour).append(randomStr).toString();
        CommonTools.write2Local(dirFile, anjukeList);
    }

    @Override
    public void process(Page page) {
        String curUrl = page.getUrl().get();
        try {
            requests++;
            if (curUrl.equals(firUrl)) {
                List<String> all = page.getHtml().xpath("//div[@class='city-mod']/dl/html()").all();
//            new Html(page.getHtml().xpath("//div[@class='city-mod']/dl/html()").all().get(0)).xpath("//dd/a/@href").all()
                for (String dl : all) {
                    List<String> as = new Html(dl).xpath("//dd/html()").all();
                    for (String a : as) {
                        Html aHtml = new Html(a);
                        String soj = aHtml.xpath("//a/@soj").get();
                        String href = aHtml.xpath("//a/@href").get();
                        String city = aHtml.xpath("//a/text()").get();
                        page.addTargetRequest(new Request(new StringBuffer(href).append(firPageUrlTmp).append(soj).toString()).putExtra(cityKeyParam, city));
                    }

                }
            } else if (curUrl.contains(firPageUrlTmp)) {
                /**
                 * https://qd.fang.anjuke.com/?from=AF_Home_switchcity
                 * 处理第一页
                 */
                List<String> pages = page.getHtml().xpath("//div[@class='pagination']/a/@href").all();
                if (pages.size() > 3) {
                    page.addTargetRequest(new Request(pages.get(0)).putExtra(cityKeyParam, page.getRequest().getExtra(cityKeyParam)));
                }
                addDetailUrlInListPage(page);
            } else if (curUrl.contains(afterFirPageUrlTmp)) {
                /**
                 * 处理列表页非第一页的页面 https://qd.fang.anjuke.com/loupan/all/p2/
                 */
                //提取下一个页面url添加到队列
                addNextListPageUrl(curUrl, page);

                //提前详情url添加到队列
                addDetailUrlInListPage(page);
            } else if (curUrl.contains(detailUrlTmp)) {
                /**
                 * 处理详情
                 */
                Html html = page.getHtml();
                AnJuKeLouPan anJuKeLouPan = new AnJuKeLouPan();
                String id = curUrl.substring(curUrl.lastIndexOf("/") + 1, curUrl.lastIndexOf(".html"));
                String url = curUrl.substring(0, curUrl.lastIndexOf("?"));
                String city = page.getRequest().getExtra(cityKeyParam).toString();
                String district = html.xpath("//div[@class='crumb-item fl']/a[3]/text()").get();//区
                String index = page.getRequest().getExtra(indexKeyParam).toString();
                String pageNo = page.getRequest().getExtra(pageKeyParam).toString();
                String name = html.xpath("//div[@class='lp-tit']/h1[@id='j-triggerlayer']/text()").get();
                String status = html.xpath("//div[@class='lp-tit']/i/text()").get();
                String priceText = html.xpath("//dl[@class='basic-parms clearfix']/dd[@class='price']/p/text()").get();
                String unit = "square";
                if (StringUtils.isNotEmpty(priceText)) {
                    priceText = priceText.trim().replace(" ", "");
                } else {
                    priceText = "";
                }
                String priceStr = html.xpath("//dl[@class='basic-parms clearfix']/dd[@class='price']/p/em/text()").get();
                String otherPriceStr = html.xpath("//dl[@class='basic-parms clearfix']/dd[@class='price']/span[@class='other']/em/text()").get();
                int intOtherPrice = 0;
                if (StringUtils.isNotEmpty(otherPriceStr)) {
                    intOtherPrice = Integer.parseInt(otherPriceStr);
                    if (otherPriceStr.contains("万元")) {
                        intOtherPrice = intOtherPrice * 10000;
                        unit = "suite";
                    }
                }
                int intPrice = 0;
                int intAroundPrice = 0;
                String aroundPriceStr = "";
                if (StringUtils.isNotEmpty(priceStr)) {
                    intPrice = Integer.parseInt(priceStr);
                    if (priceText.contains("万元")) {
                        intPrice = intPrice * 10000;
                    }
                } else {
                    aroundPriceStr = html.xpath("//dl[@class='basic-parms clearfix']/dd[@class='around-price']/text()").get();
                    intAroundPrice = Integer.parseInt(html.xpath("//dl[@class='basic-parms clearfix']/dd[@class='around-price']/span/text()").get());
                    if (aroundPriceStr.contains("万元")) {
                        intAroundPrice = intAroundPrice * 10000;
                    }
                }
                String advantage = html.xpath("//dl[@class='basic-parms clearfix']//span[@class='lpAddr-text']/text()").get();
                String ajust = html.xpath("//dl[@class='basic-parms clearfix']/dd[@class='ajust']/div[@class='house-item']/a/text()").get();//户型
                String location = html.xpath("//dl[@class='basic-parms clearfix']//span[@class='lpAddr-text']/text()").get();
                if (StringUtils.isNotEmpty(location)) {
                    location = location.trim();
                }
                String openDate = html.xpath("//div[@class='brief-info basic-parms']/ul[@class='info-left']/li[1]/span/text()").get();
                String openDateFormat = "";
                if (StringUtils.isNotEmpty(openDate)) {
                    openDateFormat = openDate.trim().replaceAll("年|月|日", "");
                }
                String closeDate = html.xpath("//div[@class='brief-info basic-parms']/ul[@class='info-right']/li[1]/span/text()").get();
                String closeDateFormat = "";
                if (StringUtils.isNotEmpty(closeDate)) {
                    closeDateFormat = closeDate.trim().replaceAll("年|月|日", "");
                }
                String lastActionTime = "";
                String lastActionTitle = "";
                String lastActionContent = "";
                try {
                    lastActionTime = html.xpath("//div[@class='fl short']/div[@class='mod']/div[@class='inner-trend']/ul/li[@class='bdbot']/div/span/text()").get();
                    if (StringUtils.isNotEmpty(lastActionTime)) {
                        lastActionTime = lastActionTime.trim();
                    }
                    lastActionTitle = html.xpath("//div[@class='fl short']/div[@class='mod']/div[@class='inner-trend']/ul/li[@class='bdbot']/div/a/text()").get();
                    if (StringUtils.isNotEmpty(lastActionTitle)) {
                        lastActionTitle = lastActionTitle.trim();
                    }
                    lastActionContent = html.xpath("//div[@class='fl short']/div[@class='mod']/div[@class='inner-trend']/ul/li[@class='bdbot']/div/p/text()").get();
                    if (StringUtils.isNotEmpty(lastActionContent)) {
                        lastActionContent = lastActionContent.trim();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                anJuKeLouPan.setId(id);
                anJuKeLouPan.setCity(city);
                anJuKeLouPan.setDistrict(district);
                anJuKeLouPan.setIndex(index);
                anJuKeLouPan.setPageNo(pageNo);
                anJuKeLouPan.setName(name);
                anJuKeLouPan.setStatus(status);
                anJuKeLouPan.setPriceText(priceText);
                anJuKeLouPan.setPriceStr(priceStr);
                anJuKeLouPan.setOtherPriceStr(otherPriceStr);
                anJuKeLouPan.setUnit(unit);
                anJuKeLouPan.setIntPrice(intPrice);
                anJuKeLouPan.setIntOtherPrice(intOtherPrice);
                anJuKeLouPan.setAroundPriceStr(aroundPriceStr);
                anJuKeLouPan.setIntAroundPrice(intAroundPrice);
                anJuKeLouPan.setAdvantage(advantage);
                anJuKeLouPan.setAjust(ajust);
                anJuKeLouPan.setLocation(location);
                anJuKeLouPan.setOpenDate(openDate);
                anJuKeLouPan.setOpenDateFormat(openDateFormat);
                anJuKeLouPan.setCloseDate(closeDate);
                anJuKeLouPan.setCloseDateFormat(closeDateFormat);
                anJuKeLouPan.setLastActionTime(lastActionTime);
                anJuKeLouPan.setLastActionTitle(lastActionTitle);
                anJuKeLouPan.setLastActionContent(lastActionContent);
                anJuKeLouPan.setUrl(url);
                anjukeList.add(anJuKeLouPan.toString());
            }
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.error("execute faild,url:" + curUrl);
            e.printStackTrace();
            if (++exCnt % 100 == 0) {
                MailTools.sendAlarmmail("huyadetailanchor 异常请求个数过多", "url: " + failedUrl.toString());
//                System.exit(1);
            }
        }
    }

    private void addNextListPageUrl(String curUrl, Page page) {
        int curPage = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf("p") + 1).replace("/", ""));
        List<String> allPages = page.getHtml().xpath("//div[@class='pagination']/a/text()").all();
        int nextPage = Integer.parseInt(allPages.get(allPages.size() - 2));
        if (nextPage > curPage) {
            page.addTargetRequest(new Request(curUrl.replace(String.valueOf(curPage), String.valueOf(++curPage))).putExtra(cityKeyParam, page.getRequest().getExtra(cityKeyParam)));
        }
    }

    private void addDetailUrlInListPage(Page page) {
        List<String> keyList = page.getHtml().xpath("//div[@class='key-list']/div/html()").all();
        //处理列表,page=1,city从cururl获取,将详情url添加到队列
        for (String item : keyList) {
            Html itemHtml = new Html(item);
            String href = itemHtml.xpath("//a/@href").get();//详情url
            String soj = itemHtml.xpath("//a/@soj").get();//AF_RANK_2
            page.addTargetRequest(new Request(new StringBuffer(href).append(detailUrlAddTmp).append(soj).toString()).putExtra(cityKeyParam, page.getRequest().getExtra(cityKeyParam)).putExtra(indexKeyParam, soj.replace(sojPre, "")).putExtra(pageKeyParam, "1"));
        }
    }

    @Override
    public Site getSite() {
        return this.site.setHttpProxy(null);
    }
}
