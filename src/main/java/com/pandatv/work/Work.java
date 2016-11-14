package com.pandatv.work;

import com.pandatv.common.Const;
import com.pandatv.processor.DouyuAnchorProccessor;
import com.pandatv.processor.HuyaAnchorProcessor;
import com.pandatv.processor.LongzhuAnchorProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by likaiqing on 2016/11/7.
 */
public class Work {
    private static final Logger logger = LoggerFactory.getLogger(Work.class);

    public static void main(String[] args) {
        if (null == args || args.length == 0) {
            return;
        }
        switch (args[0]) {
//            case Const.JINGPIN://jingpin douyu lol 20161110 4  :在四点的时候竞品抓取斗鱼的lol游戏
//                judgeParam(args,5);
//                //斗鱼竞品抓取,抓取https://www.douyu.com/directory/game/下面根据args[1]来决定的url的第一页数据
//                logger.info("JingPinProcessor.crawler start");
//                JingPinProcessor.crawler(args);
//                break;
            case Const.DOUYUANCHOR://douyuanchor 20161110 19
                judgeParam(args, 3);
                logger.info("DouyuAnchorProccessor.crawler start");
                DouyuAnchorProccessor.crawler(args);
                break;
//            case Const.DOUYUNEWLIVE://douyunewlive
//                logger.info("DouyuNewLiveProccessor.crawler start");
//                DouyuNewLiveProccessor.crawler(args);
//                break;
            case Const.HUYAANCHOR://huyaanchor 20161111 15
                judgeParam(args, 3);
                logger.info("HuyaAnchorProcessor.crawler start");
                HuyaAnchorProcessor.crawler(args);
                break;
            case Const.LONGZHUANCHOR://longzhuanchor 20161111 15
                judgeParam(args, 3);
                logger.info("LongzhuAnchorProcessor.crawler start");
                LongzhuAnchorProcessor.crawler(args);
                break;
        }
    }

    public static void judgeParam(String[] args, int count) {
        if (args.length < count) {
            logger.info("pagecount param less than 2");
            return;
        }
    }
}
