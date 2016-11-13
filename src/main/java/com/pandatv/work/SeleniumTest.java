package com.pandatv.work;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

/**
 * Created by likaiqing on 2016/11/12.
 */
public class SeleniumTest {
    public static void main(String[] args) {
        System.getProperties().setProperty("webdriver.chrome.driver","src/doc/mac/chromedriver");
        WebDriver webDriver = new ChromeDriver();
        webDriver.get("https://www.douyu.com/directory/all");
        WebElement webElement = webDriver.findElement(By.xpath("/html"));
        System.out.println(webElement.getAttribute("outerHTML"));
        webDriver.close();
    }
}
