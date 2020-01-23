package com.linch.bigdata.mapreduce.topn;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author linch90
 */
@Slf4j
public class TopNMapper extends Mapper<LongWritable, Text, TaobaoOrder, DoubleWritable> {

    private DoubleWritable valueOut = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("TopN", "invalidRecords");
        String record = value.toString();
        String[] fields = record.split("\t");
        if(fields.length == 6){
            try{
                String userId = fields[0];
                String datetime = fields[1];
                String title = fields[2];
                double unitPrice = Double.parseDouble(fields[3]);
                int purchaseNum = Integer.parseInt(fields[4]);
                String productId = fields[5];
                TaobaoOrder taobaoOrder = new TaobaoOrder(userId, getYearMonth(datetime), title, unitPrice, purchaseNum, productId);
                double totalPrice = unitPrice * purchaseNum;
                valueOut.set(totalPrice);
                context.write(taobaoOrder, valueOut);
            } catch (Exception e){
                counter.increment(1L);
                log.warn("Invalid record: " + record, e);
            }

        } else {
            counter.increment(1L);
            log.warn("Invalid record: " + record);
        }
    }

    private String getYearMonth(String datetime){
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        LocalDateTime localDateTime = LocalDateTime.parse(datetime, dateTimeFormatter);
        return localDateTime.getYear() + "" + localDateTime.getMonthValue();
    }
}
