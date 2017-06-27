#!/bin/sh

day=$1
week=$2
DATE=`date +%Y%m%d -d "-$(($day+1+7*$week)) day"`
DEAL_DATE=`date +%Y%m%d -d "-$(($day+8+7*$week)) day"`
LAST_MONTH_DATE=`date +%Y%m%d -d "-28 day"`
echo $DATE
echo $DEAL_DATE
PT=${DATE}000000
LAST_PT=${DEAL_DATE}000000

HOUSE_DEAL=house_deal.txt # 近一个月成交
HOUSE_ONSALE=house_onsale.txt # 当前所有挂牌
HOUSE_HOT=house_showing.txt # 近一周房屋线下带看次数

result_dir="data/$DATE"
if [ ! -d $result_dir ];then
    mkdir $result_dir
fi

function get_house_deal
{
    # 近一个月的成交房源
    SQL="SELECT house.house_pkid, contract.state, contract.deal_time, contract.realmoney,
        house.total_prices, house.build_area, house.created_time, resblock_id
        FROM
        (SELECT * from data_center.dim_merge_contract_day
        WHERE pt='${PT}' and deal_time > '${LAST_MONTH_DATE}' and biz_type='200200000001'
        and state in ('200800000003', '200800000004')) contract
        JOIN
        (SELECT * from data_center.dim_merge_house_day
        WHERE pt = '${PT}') house
        ON (substr(house.house_pkid,-8)=substr(contract.house_pkid, -8))
        and house.hdic_house_id=contract.hdic_house_id and house.city_id = contract.city_id
         "
    hive -e "${SQL}"
}

function get_house_list_all
{
    # 目前在售房源
    SQL="SELECT house_pkid, total_prices, build_area, state, created_time, resblock_id, hdic_house_id
         FROM data_center.dim_merge_house_day
         WHERE pt='${PT}' and biz_type='200200000001' and
         (state='200100000001' or state='200100000002' or state='200100000003' or state='200100000004' or state='200100000005');
         "
    hive -e "${SQL}"
}

# 数据需要重新拉取!!
function get_hot_house
{
    # 近一周热度数据(房屋线下带看次数)
    SQL="SELECT *
         FROM data_center.hot_bd_dw_house_showing_num_day
         WHERE pt <='${PT}' and pt > '${LAST_PT}';
         "
    hive -e "${SQL}"
}

get_house_deal > ./$result_dir/${HOUSE_DEAL}
get_house_list_all > ./$result_dir/${HOUSE_ONSALE}
get_hot_house > ./$result_dir/${HOUSE_HOT}