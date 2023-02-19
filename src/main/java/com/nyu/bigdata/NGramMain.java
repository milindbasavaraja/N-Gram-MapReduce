package com.nyu.bigdata;

import com.nyu.bigdata.Mapper.BiGramLMProbabilityMapper;
import com.nyu.bigdata.Mapper.ExtraCreditMapper;
import com.nyu.bigdata.Mapper.TransformUniBiGramMapper;
import com.nyu.bigdata.Mapper.UniBiGramCountMapper;
import com.nyu.bigdata.Partitioner.CustomPartitioner;
import com.nyu.bigdata.Partitioner.CustomSecondSorter;
import com.nyu.bigdata.Partitioner.GroupComparator;
import com.nyu.bigdata.Reducer.BiGramLMProbabilityReducer;
import com.nyu.bigdata.Reducer.ExtraCreditReducer;
import com.nyu.bigdata.Reducer.TransformUniBiGramReducer;
import com.nyu.bigdata.Reducer.UniBiGramCountReducer;
import com.nyu.bigdata.model.StringPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NGramMain {

    public enum CustomCounter {
        TOTAL_UNI_GRAM_COUNT,
        TOTAL_BI_GRAM_COUNT
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //Job to count Uni-Bi-Gram words
        Configuration uniGramConf = new Configuration();

        Job uniBiGramWordCountJob = Job.getInstance(uniGramConf, "UniGram Word Count");
        uniBiGramWordCountJob.setJarByClass(NGramMain.class);
        uniBiGramWordCountJob.setMapperClass(UniBiGramCountMapper.class);
        uniBiGramWordCountJob.setReducerClass(UniBiGramCountReducer.class);
        uniBiGramWordCountJob.setOutputKeyClass(Text.class);
        uniBiGramWordCountJob.setOutputValueClass(LongWritable.class);
        uniBiGramWordCountJob.setNumReduceTasks(3);


        Path inputFilePath = new Path(args[1]);
        Path outputFilePath = new Path(args[2]);
        FileInputFormat.addInputPath(uniBiGramWordCountJob, inputFilePath);
        FileOutputFormat.setOutputPath(uniBiGramWordCountJob, outputFilePath);
        uniBiGramWordCountJob.waitForCompletion(true);

        Counter totalUniGramsCount = uniBiGramWordCountJob.getCounters().findCounter(CustomCounter.TOTAL_UNI_GRAM_COUNT);
        Counter totalBiGramsCount = uniBiGramWordCountJob.getCounters().findCounter(CustomCounter.TOTAL_BI_GRAM_COUNT);


        //Job to concatenate BiGram details with UniGram details.
        Configuration aggrUniGramCount = new Configuration();

        Job aggregatingUniGramCount = Job.getInstance(aggrUniGramCount, "Aggregating UniGramCount");
        aggregatingUniGramCount.getConfiguration().setLong(CustomCounter.TOTAL_UNI_GRAM_COUNT.name(), totalUniGramsCount.getValue());
        aggregatingUniGramCount.getConfiguration().setLong(CustomCounter.TOTAL_BI_GRAM_COUNT.name(), totalBiGramsCount.getValue());

        aggregatingUniGramCount.setJarByClass(NGramMain.class);
        aggregatingUniGramCount.setMapperClass(TransformUniBiGramMapper.class);
        aggregatingUniGramCount.setReducerClass(TransformUniBiGramReducer.class);
        aggregatingUniGramCount.setPartitionerClass(CustomPartitioner.class);
        aggregatingUniGramCount.setSortComparatorClass(CustomSecondSorter.class);
        aggregatingUniGramCount.setGroupingComparatorClass(GroupComparator.class);
        aggregatingUniGramCount.setOutputKeyClass(StringPair.class);
        aggregatingUniGramCount.setOutputValueClass(Text.class);
        aggregatingUniGramCount.setNumReduceTasks(3);


        Path inputFilePathForBiGram = new Path(args[2]);
        Path outputFilePathForBiGram = new Path(args[3]);
        FileInputFormat.addInputPath(aggregatingUniGramCount, inputFilePathForBiGram);
        FileOutputFormat.setOutputPath(aggregatingUniGramCount, outputFilePathForBiGram);

        aggregatingUniGramCount.waitForCompletion(true);


        // Job to calculate probability
        Configuration probabilityBiGramConf = new Configuration();

        Job probabilityBiGramJob = Job.getInstance(probabilityBiGramConf, "BiGram LM Probability");
        probabilityBiGramJob.getConfiguration().setLong(CustomCounter.TOTAL_BI_GRAM_COUNT.name(), totalBiGramsCount.getValue());
        probabilityBiGramJob.getConfiguration().setLong(CustomCounter.TOTAL_UNI_GRAM_COUNT.name(), totalUniGramsCount.getValue());
        probabilityBiGramJob.setJarByClass(NGramMain.class);
        probabilityBiGramJob.setMapperClass(BiGramLMProbabilityMapper.class);
        probabilityBiGramJob.setNumReduceTasks(3);
        probabilityBiGramJob.setReducerClass(BiGramLMProbabilityReducer.class);
        probabilityBiGramJob.setOutputKeyClass(Text.class);
        probabilityBiGramJob.setOutputValueClass(Text.class);
        Path inputFilePathForProbabilityBiGramLM = new Path(args[3]);
        Path outputFilePathForProbabilityBiGramLM = new Path(args[4]);
        FileInputFormat.addInputPath(probabilityBiGramJob, inputFilePathForProbabilityBiGramLM);
        FileOutputFormat.setOutputPath(probabilityBiGramJob, outputFilePathForProbabilityBiGramLM);
        probabilityBiGramJob.waitForCompletion(true);



        //Job to find out maximum probability of the biGram starting with states word.
        Configuration extraCreditProbability = new Configuration();

        Job extraCreditProbabilityJob = Job.getInstance(extraCreditProbability, "Extra Credits Job");
        extraCreditProbabilityJob.setJarByClass(NGramMain.class);
        extraCreditProbabilityJob.setMapperClass(ExtraCreditMapper.class);
        extraCreditProbabilityJob.setReducerClass(ExtraCreditReducer.class);

        extraCreditProbabilityJob.setNumReduceTasks(1);
        extraCreditProbabilityJob.setOutputKeyClass(Text.class);
        extraCreditProbabilityJob.setOutputValueClass(Text.class);

        Path inputFilePathForExtraCreditProbabilityJob = new Path(args[4]);
        Path outputFilePathForExtraCreditProbabilityJob = new Path(args[5]);
        FileInputFormat.addInputPath(extraCreditProbabilityJob, inputFilePathForExtraCreditProbabilityJob);
        FileOutputFormat.setOutputPath(extraCreditProbabilityJob, outputFilePathForExtraCreditProbabilityJob);
        System.exit(extraCreditProbabilityJob.waitForCompletion(true) ? 0 : 1);

    }
}
