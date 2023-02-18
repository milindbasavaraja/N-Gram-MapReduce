package com.nyu.bigdata.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


/**
 * Custom Hadoop Writable component which acts as composite key for a job
 */
public class StringPair implements WritableComparable<StringPair> {
    private Text first;
    private Text second;


    public StringPair() {
        set(new Text(), new Text());
    }

    public StringPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public StringPair(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);

    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof StringPair) {
            StringPair sp = (StringPair) o;
            return first.equals(sp.first) && second.equals(sp.second);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }


    @Override
    public int compareTo(StringPair sp) {
        int cmp = first.compareTo(sp.getFirst());
        if (cmp != 0) {
            return cmp;
        }

        return second.compareTo(sp.getSecond());
    }

    @Override
    public String toString() {
        return "StringPair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
