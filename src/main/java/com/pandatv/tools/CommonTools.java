package com.pandatv.tools;

import com.google.common.base.Joiner;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Site;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class CommonTools {
    private static final Logger logger = LoggerFactory.getLogger(CommonTools.class);

    public static String getFormatStr(String str) {
        if (StringUtils.isEmpty(str)) {
            return "";
        }
        byte[] by = str.trim().getBytes();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int j = 0;
        for (int i = 0; i < by.length; i++) {
            if (by[i] != 1) {
                out.write(by[i]);
                j++;
            }
        }
        String result = "";
        try {
            result = new String(out.toByteArray(), "UTF-8");
            out.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result.replace("\n", "").replace("\r", "");
    }

    public static List<String> getUrls(ResultItems resultItems, String job, String plat, String gameCategory) {

        List<String> results = new ArrayList<>();
        String url = resultItems.getRequest().getUrl();
        List<String> names = resultItems.get("names");
        List<String> popularities = resultItems.get("popularities");
        List<String> rids = resultItems.get("rids");
        List<String> titles = resultItems.get("titles");
        List<String> categories = resultItems.get("categories");
//        for (int i = 0; i < names.size(); i++) {
//            Anchor anchor = new Anchor();
//            String popularitiyStr = popularities.get(i);
//            int popularitiyNum = createNum(popularitiyStr);
//            String rid = rids.get(i);
//            if (rid.contains("\u0001")) {
//                logger.info("rid contains SEP,url:{},rid:{}", url, rid);
//            }
//            if (!isValidUnicode(rid)) {
//                logger.info("rid is not valid unicode,url:{},rid:{}", url, rid);
//            }
//            anchor.setRid(rid);
//            anchor.setName(names.get(i));
//            anchor.setTitle(titles.get(i));
//            anchor.setCategory(categories.get(i));
//            anchor.setPopularityStr(popularitiyStr);
//            anchor.setPopularityNum(popularitiyNum);
//            anchor.setJob(job);
//            anchor.setPlat(plat);
//            anchor.setGame(gameCategory);
//            anchor.setUrl(url);
//            String result = anchor.toString();
//            results.add(result);
//        }
        return results;
    }

    public static int createNum(String popularitiyStr) {
        if (!popularitiyStr.contains("万")) {
            return Integer.parseInt(popularitiyStr);
        }
        double num = Double.parseDouble(popularitiyStr.substring(0, popularitiyStr.indexOf('万'))) * 10000;
        return (int) num;
    }

    public static boolean isValidUnicode(String str) {
        for (int i = 0; i < str.length(); i++) {

            int c = str.codePointAt(i);
            if (c < 0x0000 || c > 0xffff) {
                return false;
            }
        }
        return true;
    }

    public static long getDouyuWeight(String weightNum) {
        if (StringUtils.isEmpty(weightNum)) {
            return 0;
        } else if (weightNum.endsWith("kg")) {
            return (long) (Double.parseDouble(weightNum.replace("kg", "")) * 1000);
        } else if (weightNum.endsWith("g")) {
            return (long) Double.parseDouble(weightNum.replace("g", ""));
        } else if (weightNum.endsWith("t")) {
            return (long) (Double.parseDouble(weightNum.replace("t", "")) * 1000000);
        } else {
            try {
                return (long) Double.parseDouble(weightNum);
            } catch (Exception e) {
                return 0;
            }
        }
    }

    public static Site getMayiSite(Site site) {
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.put("app_key", Const.appkey);
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("GMT+8"));//使用中国时间，以免时区不同导致认证错误
        paramMap.put("timestamp", format.format(new Date()));

// 对参数名进行排序
        String[] keyArray = paramMap.keySet().toArray(new String[0]);
        Arrays.sort(keyArray);

// 拼接有序的参数名-值串
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(Const.secret);
        for (String key : keyArray) {
            stringBuilder.append(key).append(paramMap.get(key));
        }

        stringBuilder.append(Const.secret);
        String codes = stringBuilder.toString();

// MD5编码并转为大写， 这里使用的是Apache codec
        String sign = org.apache.commons.codec.digest.DigestUtils.md5Hex(codes).toUpperCase();

        paramMap.put("sign", sign);

// 拼装请求头Proxy-Authorization的值，这里使用 guava 进行map的拼接
        String authHeader = "MYH-AUTH-MD5 " + Joiner.on('&').withKeyValueSeparator("=").join(paramMap);
        site.addHeader("Proxy-Authorization", authHeader);
        site.setHttpProxy(new HttpHost(Const.MAYIHOST, Const.MAYIPORT));
        return site;
    }

    public static void main(String[] args) {
//        Set<String> set = new HashSet<>();
//        set.add("test1");
//        set.add("test2");
//        set.add("test3");
//        set.add("test4");
//        set.add("test5");
//        write2Local("/Users/likaiqing/Downloads/category_test/20170811/11/",set);
        getFormatStr("七点无限火力水友赛");
    }

    public static Site getAbuyunSite(Site site) {
        site.setHttpProxy(new HttpHost(Const.ABUYUNPHOST, Const.ABUYUNPORT));
//        Authenticator.setDefault(new ProxyAuthenticator("H953ANZ8J6HW026D", "4FF963A93342BB18"));
        site.addHeader("Proxy-Authorization", "Basic " + (new BASE64Encoder()).encode((Const.GENERATORKEY + ":" + Const.GENERATORPASS).getBytes()));//PandaHttpClientGenerator
        site.addHeader("Proxy-Switch-Ip", "yes");
        return site;
    }

    public static void writeAndMail(String hivePaht, String douyufinish, Set<String> list) {
        int mailMinute = 0;
        try {
            if (list.size() > 0) {
                PandaProcessor.hive.write2(hivePaht, list, PandaProcessor.job, PandaProcessor.curMinute);
            }
            mailMinute = Integer.parseInt(PandaProcessor.mailMinuteStr) / 10;
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (PandaProcessor.mailHours.contains(PandaProcessor.hour) && (mailMinute == 0 || mailMinute == 5)) {
            MailTools.sendTaskMail(douyufinish + PandaProcessor.date + PandaProcessor.hour, PandaProcessor.from + "<-->" + DateTools.getCurDate(), (System.currentTimeMillis() - PandaProcessor.s) + "毫秒;", list.size(), PandaProcessor.timeOutUrl, PandaProcessor.failedUrl);
        }
    }

    public static void write2Local(String dirFile, Set<String> set) {
        logger.info("dirFile:{},set.size:{}", dirFile, set.size());
        if (null == set || set.size() == 0) {
            logger.error("write2Local;null==set || set.size()==0");
            return;
        }
        String dir = dirFile.substring(0, dirFile.lastIndexOf("/"));
        File file = new File(dir);
        if (!file.exists()) {
            logger.warn("!file.exists();create dir:" + dir);
            file.mkdirs();
        }
        BufferedWriter bw = IOTools.getBW(dirFile + ".txt");
        try {
            for (Object s : set) {
                bw.write(s.toString());
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOTools.closeBw(bw);
        }
        set.clear();
        PandaProcessor.writeSuccess = true;
    }
}
