package com.nyu.bigdata.Partitioner;

import com.nyu.bigdata.model.StringPair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomSecondSorter extends WritableComparator {
    protected CustomSecondSorter() {
        super(StringPair.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        StringPair sp1 = (StringPair) w1;
        StringPair sp2 = (StringPair) w2;

        return sp1.compareTo(sp2);
    }
}
