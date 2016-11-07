package work;

import common.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import processor.DouyueProcessor;

/**
 * Created by likaiqing on 2016/11/7.
 */
public class DouyuWork {
    Logger logger = LoggerFactory.getLogger(DouyuWork.class);
    public static void main(String[] args) {
        if (null == args || args.length==0){
            return;
        }
        String flag = args[0];
        switch (args[0]){
            case Const.JINGPIN:
                DouyueProcessor.competitiveProducts();
                break;
        }
    }
}
