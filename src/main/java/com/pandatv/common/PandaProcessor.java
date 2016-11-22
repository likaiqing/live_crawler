package com.pandatv.common;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.http.HttpHost;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;

import java.util.Random;

/**
 * Created by likaiqing on 2016/11/7.
 */
public abstract class PandaProcessor implements PageProcessor {
    public static String randomStr = RandomStringUtils.random(10,new char[]{'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','0','1','2','3','4','5','6','7','8','9'});
    protected Site site = Site.me()
            .setSleepTime(1000)
            .setUseGzip(true)
            .setRetryTimes(5)
            .setTimeOut(5000)
            .setCycleRetryTimes(7)
            .setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:45.0) Gecko/20100101 Firefox/45.0");
}
