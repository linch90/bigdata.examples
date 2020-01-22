package com.linch.bigdata.mapreduce.multipropsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author linch90
 */
public class PersonMapper extends Mapper<LongWritable, Text, Person, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("PersonMapper", "illegalRecords");
        String[] fields = value.toString().split("\t");
        if(fields.length == 3){
            try{
                String name = fields[0];
                int age = Integer.parseInt(fields[1]);
                int salary = Integer.parseInt(fields[2]);
                context.write(new Person(name, age, salary), NullWritable.get());
            } catch (Exception e){
                counter.increment(1L);
                e.printStackTrace();
            }
        } else{
            counter.increment(1L);
        }
    }
}
