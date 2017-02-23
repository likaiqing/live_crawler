package com.pandatv.tools;

import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by likaiqing on 2016/11/29.
 */
public class UnicodeTools {
    public static String unicodeToString(String str) {
        if (StringUtils.isEmpty(str)) return "";
        Pattern pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))");
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            ch = (char) Integer.parseInt(matcher.group(2), 16);
            str = str.replace(matcher.group(1), ch + "");
        }
        return str;
    }
    static String getUnicode(String s) {
        try {
            StringBuffer out = new StringBuffer("");
            byte[] bytes = s.getBytes("unicode");
            for (int i = 0; i < bytes.length - 1; i += 2) {
                out.append("\\u");
                String str = Integer.toHexString(bytes[i + 1] & 0xff);
                for (int j = str.length(); j < 2; j++) {
                    out.append("0");
                }
                String str1 = Integer.toHexString(bytes[i] & 0xff);
                out.append(str1);
                out.append(str);

            }
            return out.toString();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        String s = unicodeToString("{\"status\":200,\"data\":{\"1584989003\":{\"uid\":\"1423805942\",\"nick\":\"\\u66b4\\u96ea\\u6e38\\u620f\\u9891\\u90531\",\"introduction\":\"\\u9ec4\\u91d1\\u4e16\\u4ff1\\u676f\\u5c0f\\u7ec4\\u8d5bDay1\\uff08\\u4e2d\\u5348\\uff09\",\"gid\":\"1450\",\"aid\":\"23275240\",\"totalCount\":\"1862\",\"gameFullName\":\"\\u98ce\\u66b4\\u82f1\\u96c4\",\"gameHostName\":\"heroes\",\"activityCount\":\"9481\",\"avatar180\":\"http:\\/\\/huyaimg.dwstatic.com\\/avatar\\/1091\\/8f\\/f72631f796b336a21670335b67d785_180_135.jpg\",\"liveSourceType\":\"1\",\"screenType\":\"0\",\"profileHost\":\"1584989003\",\"isLive\":1}},\"msg\":\"\"}");
        System.out.println(s);
    }
}
