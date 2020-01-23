package com.linch.bigdata.mapreduce.topn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author linch90
 */
public class TaobaoOrderPartitioner extends Partitioner<TaobaoOrder, DoubleWritable> {
    @Override
    public int getPartition(TaobaoOrder taobaoOrder, DoubleWritable doubleWritable, int numReduceTasks) {
        return (taobaoOrder.getUserId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
