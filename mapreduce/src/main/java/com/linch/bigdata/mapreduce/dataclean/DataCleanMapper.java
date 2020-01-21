package com.linch.bigdata.mapreduce.dataclean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author linch90
 */
public class DataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private NullWritable nullValue = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("DataClean", "damagedRecords");

        String line = value.toString();
        String[] fields = line.split("\t");
        if(fields.length != 6){
            counter.increment(1L);
        } else {
            context.write(value, nullValue);
        }
    }
}
