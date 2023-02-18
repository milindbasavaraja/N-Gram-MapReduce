package com.nyu.bigdata.Partitioner;

import com.nyu.bigdata.model.StringPair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * GroupComparator to sort the keys and if keys are same them group them.
 */
public class GroupComparator extends WritableComparator {

    protected GroupComparator(){
        super(StringPair.class,true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        StringPair sp1 = (StringPair) w1;
        StringPair sp2 = (StringPair) w2;

        return sp1.getFirst().compareTo(sp2.getFirst());
    }
}
