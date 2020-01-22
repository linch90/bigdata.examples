package com.linch.bigdata.mapreduce.multipropsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author linch90
 */
public class PersonReducer extends Reducer<Person, NullWritable, Person, NullWritable> {
    @Override
    protected void reduce(Person key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
