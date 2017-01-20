#!/bin/bash

#!/bin/bash

date=$1
date=${date:=`date -d "yesterday" +%Y%m%d`}

hive -e "
insert overwrite table panda_competitor_result.plat_day_changed_analyse_by_sameday partition(par_date)
SELECT
  plat,
  sum(pcu)               pcu,
  sum(last_followers)    last_followers,
  sum(last_weight)       last_weight,
  sum(followers_changed) followers_changed,
  sum(weight_changed)    weight_changed,
  '$date'
FROM
  panda_competitor_result.anchor_day_changed_analyse_by_sameday
WHERE par_date = '$date'
GROUP BY plat;
"