package com.pandatv.downloader.selenium;

import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.downloader.Downloader;
import us.codecraft.webmagic.selector.Html;
import us.codecraft.webmagic.selector.PlainText;
import us.codecraft.webmagic.utils.UrlUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Created by likaiqing on 2016/11/10.
 */
public class SeleniumDownloader implements Downloader, Closeable {
    private volatile WebDriverPool webDriverPool;

    private Logger logger = Logger.getLogger(getClass());

    private int poolSize = 1;

    /**
     * 新建
     *
     * @param chromeDriverPath chromeDriverPath
     */
    public SeleniumDownloader(String chromeDriverPath) {
        System.getProperties().setProperty("webdriver.chrome.driver",
                chromeDriverPath);
    }

    @Override
    public Page download(Request request, Task task) {
        checkInit();
        WebDriver webDriver;
        try {
            webDriver = webDriverPool.get();
        } catch (InterruptedException e) {
            logger.warn("interrupted", e);
            return null;
        }
        logger.info("downloading page " + request.getUrl());
        webDriver.get(request.getUrl());
        WebDriver.Options manage = webDriver.manage();
        Site site = task.getSite();
        if (site.getCookies() != null) {
            for (Map.Entry<String, String> cookieEntry : site.getCookies()
                    .entrySet()) {
                Cookie cookie = new Cookie(cookieEntry.getKey(),
                        cookieEntry.getValue());
                manage.addCookie(cookie);
            }
        }

		/*
		 * TODO You can add mouse event or other processes
		 *
		 * @author: bob.li.0718@gmail.com
		 */

        WebElement webElement = webDriver.findElement(By.xpath("/html"));
        String content = webElement.getAttribute("outerHTML");
        Page page = new Page();
        page.setRawText(content);
        page.setHtml(new Html(UrlUtils.fixAllRelativeHrefs(content,
                request.getUrl())));
        page.setUrl(new PlainText(request.getUrl()));
        page.setRequest(request);
        webDriverPool.returnToPool(webDriver);
        return page;
    }

    private void checkInit() {
        if (webDriverPool == null) {
            synchronized (this) {
                webDriverPool = new WebDriverPool(poolSize);
            }
        }
    }

    @Override
    public void setThread(int thread) {
        this.poolSize = thread;
    }

    @Override
    public void close() throws IOException {
        webDriverPool.closeAll();
    }
}
