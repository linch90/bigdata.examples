package com.linch.bigdata.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopNReducer extends Reducer<TaobaoOrder, DoubleWritable, Text, DoubleWritable> {

    private Text keyOut = new Text();

    @Override
    protected void reduce(TaobaoOrder key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int numTopN = conf.getInt("numTopN", 2);
        int num = 0;
        for(DoubleWritable value : values){
            if(num < numTopN) {
                keyOut.set(key.getUserId() + " " + key.getYearMonth());
                context.write(keyOut, value);
            }
            num++;
        }
    }
}
