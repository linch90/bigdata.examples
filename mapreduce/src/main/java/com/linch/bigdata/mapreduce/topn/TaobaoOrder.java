package com.linch.bigdata.mapreduce.topn;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author linch90
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaobaoOrder implements WritableComparable<TaobaoOrder> {
    private String userId;

    private String yearMonth;

    private String title;

    private double unitPrice;

    private int purchaseNum;

    private String productId;

    @Override
    public int compareTo(TaobaoOrder o) {
        int userIdCompare = userId.compareTo(o.userId);
        if(userIdCompare == 0){
            int yearMonthCompare = yearMonth.compareTo(o.yearMonth);
            if(yearMonthCompare == 0){
                Double thisTotalPrice = unitPrice * purchaseNum;
                Double otherTotalPrice = o.unitPrice * o.purchaseNum;
                return - thisTotalPrice.compareTo(otherTotalPrice);
            } else {
                return yearMonthCompare;
            }
        } else {
            return userIdCompare;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeUTF(yearMonth);
        out.writeUTF(title);
        out.writeDouble(unitPrice);
        out.writeInt(purchaseNum);
        out.writeUTF(productId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readUTF();
        yearMonth = in.readUTF();
        title = in.readUTF();
        unitPrice = in.readDouble();
        purchaseNum = in.readInt();
        productId = in.readUTF();
    }
}
