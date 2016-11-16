package com.pandatv.downloader.selenium.seleniumtest;

import com.pandatv.downloader.selenium.WebDriverPool;
import org.junit.Test;
import org.openqa.selenium.WebDriver;

/**
 * Created by likaiqing on 2016/11/16.
 */
public class WebDriverPoolTest {
    private String chromeDriverPath = "src/doc/mac/chromedriver";

//    @Ignore("need chrome driver")
    @Test
    public void test() {
        System.getProperties().setProperty("webdriver.chrome.driver", chromeDriverPath);
        WebDriverPool webDriverPool = new WebDriverPool(5);
        for (int i = 0; i < 5; i++) {
            try {
                WebDriver webDriver = webDriverPool.get();
                System.out.println(i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        webDriverPool.closeAll();
    }
}
