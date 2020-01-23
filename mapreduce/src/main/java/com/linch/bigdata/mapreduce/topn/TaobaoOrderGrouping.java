package com.linch.bigdata.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaobaoOrderGrouping extends WritableComparator {
    public TaobaoOrderGrouping(){
        super(TaobaoOrder.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TaobaoOrder aTaobaoOrder = (TaobaoOrder) a;
        TaobaoOrder bTaobaoOrder = (TaobaoOrder) b;
        int userIdCompare = aTaobaoOrder.getUserId().compareTo(bTaobaoOrder.getUserId());
        if(userIdCompare == 0){
            return aTaobaoOrder.getYearMonth().compareTo(bTaobaoOrder.getYearMonth());
        } else {
            return userIdCompare;
        }
    }
}
