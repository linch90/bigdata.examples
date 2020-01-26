package com.linch.bigdata.mapreduce.utils;

import lombok.Getter;
import org.apache.hadoop.io.Text;

/**
 * @author linch90
 */
@Getter
public class NoaaRecordParser {

    private final static int MISSING_TEMPERATURE = 9999;

    private String stationIdentifier;

    private String datetimeString;

    private String airTemperatureString;

    private int airTemperature;

    private boolean airTemperatureMalformed;

    private String quality;

    public void parse(String record){
        stationIdentifier = record.substring(4, 10) + "-" +record.substring(10, 15);
        datetimeString = record.substring(15, 27);
        char signFlag = record.charAt(87);
        if(signFlag == '+'){
            airTemperatureString = record.substring(88, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else if (signFlag == '-') {
            airTemperatureString = record.substring(87, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else {
            airTemperatureMalformed = true;
        }

        quality = record.substring(92, 93);
    }

    public void parse(Text record){
        parse(record.toString());
    }

    public boolean isValidTemperature(){
        return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }
}
