package com.pandatv.downloader.selenium.seleniumtest;

import com.pandatv.downloader.selenium.WebDriverPool;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

/**
 * Created by likaiqing on 2016/11/16.
 */
public class WebDriverPoolTest {
    private static String chromeDriverPath = "/data/tmp/crawler_driver/chromedriver";

//    @Ignore("need chrome driver")
//    @Test
    public static void crawler(String[] args) {
        System.getProperties().setProperty("webdriver.chrome.driver", chromeDriverPath);
        WebDriverPool webDriverPool = new WebDriverPool(5);
        for (int i = 0; i < 1; i++) {
            try {
                WebDriver webDriver = webDriverPool.get();
                webDriver.get("https://www.douyu.com/directory/all");
                WebElement webElement = webDriver.findElement(By.xpath("/html"));
                System.out.println(webElement.getAttribute("outerHTML"));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        webDriverPool.closeAll();
    }
}
