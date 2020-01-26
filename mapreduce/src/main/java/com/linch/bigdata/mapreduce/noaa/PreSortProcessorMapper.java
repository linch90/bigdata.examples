package com.linch.bigdata.mapreduce.noaa;

import com.linch.bigdata.mapreduce.utils.NoaaRecordParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author linch90
 */
public class PreSortProcessorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private NoaaRecordParser parser = new NoaaRecordParser();
    private IntWritable temperature = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if(parser.isValidTemperature()){
            temperature.set(parser.getAirTemperature());
            context.write(temperature, value);
        }
    }
}
