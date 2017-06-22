#!/bin/bash
source conf/conf.sh $1 $2

function get_follow_nums
{
    # 获取 (LAST_PT, PT] 房源关注数据
    SQL="SELECT favorite_condition as favorite_condition
        , count(1) as follow_num
        FROM stg.stg_lianjia_lianjia_user_favorite_da
        WHERE pt='${PT}' and favorite_type='ershoufang'
        and bit_status=1
        and CONCAT(SUBSTR(ctime,1,4),SUBSTR(ctime,6,2),SUBSTR(ctime,9,2),'000000') = '${PT}'
        group by favorite_condition;
    "
    hive -e "${SQL}"
}

function get_view_nums
{
    # 获取 (LAST_PT, PT] 房源浏览数据
    SQL="SELECT house_code as house_code
        , delta as pvnum
        FROM data_center.yzd_house_hot_pv_di
        WHERE pt = '${PT}'
        and channel = 'ershoufang'
        and delta <> '0';
    "
    hive -e "${SQL}"
}

function get_show_nums
{
    # 获取 (LAST_PT, PT] 房源带看数据
    SQL="SELECT house_code as house_code
        , count(1) as show_num
        FROM stg.stg_lianjia_house_showing_house_da
        WHERE pt = '${PT}'
        and status = 1
        and audit_status in (0,1,4)
        and (reciprocal_frame = 3 or reciprocal_frame = 0)
        and customer_code <> ''
        and house_code <> ''
        and CONCAT(SUBSTR(ctime,1,4),SUBSTR(ctime,6,2),SUBSTR(ctime,9,2),'000000') = '${PT}'
        group by house_code;
    "
    hive -e "${SQL}"
}

get_follow_nums > ${HOUSE_FOLLOW}
get_view_nums > ${HOUSE_VIEW}
get_show_nums > ${HOUSE_SHOW}
