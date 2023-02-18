package com.nyu.bigdata.Partitioner;

import com.nyu.bigdata.model.StringPair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * CustomSecondSorter to sort the groupings when the key is same.
 * This ensures that the reducer - TransformUniBiGramReducer, always receives UniGram and its count
 * before the arrival of BiGram which will help us to concatenate the count of UniGram to the BiGram.
 */
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
