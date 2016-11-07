package processor;

import pipeline.DouyuePipeLine;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;

/**
 * Created by likaiqing on 2016/11/7.
 */
public class DouyueProcessor extends PandaProcessor {
    public void process(Page page) {
        String url = page.getUrl().toString();
        page.getHtml();
    }

    public Site getSite() {
        return site;
    }

    public static void competitiveProducts() {
        String dyLol = "https://www.douyu.com/directory/game/LOL";
        Spider.create(new DouyueProcessor()).addUrl(dyLol).thread(1).addPipeline(new DouyuePipeLine()).run();
    }
}
