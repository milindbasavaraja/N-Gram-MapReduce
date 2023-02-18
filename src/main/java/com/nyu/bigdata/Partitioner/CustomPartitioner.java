package com.nyu.bigdata.Partitioner;

import com.nyu.bigdata.model.StringPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Custom Partitioner for the custom composite key of type StringPair
 * Partition is done base on first part of composite key which is UniGram.
 *
 */
public class CustomPartitioner extends Partitioner<StringPair, Text> {


    @Override
    public int getPartition(StringPair stringPair, Text text, int numOfPartitions) {
        return Math.abs(stringPair.getFirst().hashCode() * 127) % numOfPartitions;
    }
}
