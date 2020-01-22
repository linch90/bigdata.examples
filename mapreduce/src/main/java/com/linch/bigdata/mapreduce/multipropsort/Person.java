package com.linch.bigdata.mapreduce.multipropsort;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
public class Person implements WritableComparable<Person> {
    private String name;

    private int age;

    private int salary;

    @Override
    public int compareTo(Person o) {
        int salaryCompare = this.salary - o.salary;
        if(salaryCompare != 0){
            return -salaryCompare;
        } else{
            return this.age - o.age;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(age);
        out.writeInt(salary);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        age = in.readInt();
        salary = in.readInt();
    }
}
