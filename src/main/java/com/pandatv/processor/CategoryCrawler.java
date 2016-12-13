package com.pandatv.processor;

import com.pandatv.common.PandaProcessor;
import com.pandatv.tools.CommonTools;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;

/**
 * Created by likaiqing on 2016/12/13.
 */
public class CategoryCrawler extends PandaProcessor {
    @Override
    public void process(Page page) {

    }

    @Override
    public Site getSite() {
        return CommonTools.getAbuyunSite(site).setSleepTime(1000);
    }
}
