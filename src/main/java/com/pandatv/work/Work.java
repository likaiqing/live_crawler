package com.pandatv.work;

import com.pandatv.common.Const;
import com.pandatv.processor.*;
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
            case Const.DOUYUANCHOR://douyuanchor 20161110 19
                judgeParam(args, 3);
                logger.info("DouyuAnchorProccessor.crawler start");
                DouyuAnchorProccessor.crawler(args);
                break;
            case Const.DOUYUNEWLIVE://douyunewlive
                judgeParam(args, 3);
                logger.info("DouyuNewLiveProccessor.crawler start");
                DouyuNewLiveProccessor.crawler(args);
                break;
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
            case Const.QUANMINANCHOR://quanminanchor 20161111 15
                judgeParam(args, 3);
                logger.info("QuanminAnchorProcessor.crawler start");
                QuanminAnchorProcessor.crawler(args);
                break;
            case Const.ZHANQIANCHOR://zhanqianchor 20161111 15
                judgeParam(args, 3);
                logger.info("ZhanqiAnchorProcessor.crawler start");
                ZhanqiAnchorProcessor.crawler(args);
                break;
            case Const.CHUSHOUANCHOR://chushouanchor 20161111 15
                judgeParam(args, 3);
                logger.info("ChushouAnchorProcessor.crawler start");
                ChushouAnchorProcessor.crawler(args);
                break;
            case Const.DOUYUDETAILANCHOR://douyudetailanchor 20161111 15
                judgeParam(args, 3);
                logger.info("DouyuDetailAnchorProcessor.crawler start");
                DouyuDetailAnchorProcessor.crawler(args);
                break;
            case Const.HUYADETAILANCHOR://huyadetailanchor 20161111 15
                judgeParam(args, 3);
                logger.info("HuyaDetailAnchorProcessor.crawler start");
                HuyaDetailAnchorProcessor.crawler(args);
                break;
            case Const.INDEXREC://斗鱼和虎牙首页推荐的douyuindexrec  huyaindexrec
                judgeParam(args, 3);
                logger.info("ChushouAnchorProcessor.crawler start");
                IndexRecProcessor.crawler(args);
                break;
            case Const.EXPORT2EXCEL://斗鱼和虎牙首页推荐的douyuindexrec  huyaindexrec
                logger.info("ChushouAnchorProcessor.crawler start");
                judgeParam(args, 6);
                ExportData.export2Excel(args);
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
