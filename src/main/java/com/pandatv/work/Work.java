package com.pandatv.work;

import com.pandatv.common.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pandatv.processor.JingPinProcessor;

/**
 * Created by likaiqing on 2016/11/7.
 */
public class Work {
    private static final Logger logger = LoggerFactory.getLogger(Work.class);
    public static void main(String[] args) {
        if (null == args || args.length==0){
            return;
        }
        String flag = args[0];
        switch (args[0]){//jingpin douyu lol 4  :在四点的时候竞品抓取斗鱼的lol游戏
            case Const.JINGPIN:
                judgeParam(args,4);
                //斗鱼竞品抓取,抓取https://www.douyu.com/directory/game/下面根据args[1]来决定的url的第一页数据
                JingPinProcessor.competitiveProducts(args);
                break;
            case Const.DOUYUANCHOR:

        }
    }
    public static void judgeParam(String[] args,int count){
        if (args.length<count){
            logger.info("pagecount param less than 2");
            return;
        }
    }
}
