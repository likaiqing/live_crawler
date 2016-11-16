package com.pandatv.work;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by likaiqing on 2016/11/12.
 */
public class SeleniumTest {
    private static final Logger logger = LoggerFactory.getLogger(SeleniumTest.class);
    public static void crawler(String[] args) {
        logger.info("start setProperty");
        Map<String,Object> chromeOptions = new HashMap<>();
        chromeOptions.put("binary","/opt/googl/chrome");
        System.getProperties().setProperty("webdriver.chrome.driver","/data/tmp/crawler_driver/chromedriver");
        WebDriver webDriver = new ChromeDriver();
        webDriver.get("https://www.douyu.com/directory/all");
        WebElement webElement = webDriver.findElement(By.xpath("/html"));
        System.out.println(webElement.getAttribute("outerHTML"));
        webDriver.close();
    }
}
