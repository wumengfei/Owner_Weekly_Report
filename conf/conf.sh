#!/bin/bash

day=$1
week=$2

export DATE=`date +%Y%m%d -d "-$(($day+1+7*$week)) day"`
export LAST_DATE=`date +%Y%m%d -d "-$(($day+8+7*$week)) day"`

export PT=${DATE}000000
export LAST_PT=${LAST_DATE}000000

export result_dir="data/${DATE}"
if [ ! -d $result_dir ];then
    mkdir $result_dir
fi

export HOUSE_FOLLOW=result_dir/follow_nums.txt
export HOUSE_VIEW=result_dir/view_nums.txt
export HOUSE_SHOW=result_dir/show_nums.txt