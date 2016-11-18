package com.pandatv.common;

import org.apache.http.HttpHost;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;

/**
 * Created by likaiqing on 2016/11/7.
 */
public abstract class PandaProcessor implements PageProcessor {
    protected Site site = Site.me()
            .setSleepTime(300)
            .setUseGzip(true)
            .setRetryTimes(5)
            .setTimeOut(5000)
            .setCycleRetryTimes(7)
            .setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:45.0) Gecko/20100101 Firefox/45.0");
}
