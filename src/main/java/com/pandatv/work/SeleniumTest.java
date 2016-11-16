package com.pandatv.work;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.remote.DesiredCapabilities;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by likaiqing on 2016/11/12.
 */
public class SeleniumTest {
    public static void main(String[] args) {
        System.getProperties().setProperty("webdriver.chrome.driver", "src/doc/mac/chromedriver");
        Map<String, Object> contentSettings = new HashMap<String, Object>();
        contentSettings.put("images", 2);

        Map<String, Object> preferences = new HashMap<String, Object>();
        preferences.put("profile.default_content_settings", contentSettings);

        DesiredCapabilities caps = DesiredCapabilities.chrome();
        caps.setCapability("chrome.prefs", preferences);
        caps.setCapability("chrome.switches", Arrays.asList("--user-data-dir=/Users/yihua/temp/chrome"));
        WebDriver webDriver = new ChromeDriver(caps);
        webDriver.get("http://huaban.com/");
        WebElement webElement = webDriver.findElement(By.xpath("/html"));
        System.out.println(webElement.getAttribute("outerHTML"));
        webDriver.close();
    }
}
