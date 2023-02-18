package com.nyu.bigdata.Mapper;

import com.nyu.bigdata.NGramMain;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class is responsible for finding out the Probabilities.
 * Input ---> key: LongWritable, Value ----> Text
 * Eg: The Input value will look as follows:
 *  united states 2107	united 2735
 *  Explanation of input
 *  1.  String "united states 2107" states "united states" as BiGram and 2107 is the count of the BiGram.
 *  2.  String "united 2735" states "united" as UniGram and 2735 is the count of UniGram.
 */
public class BiGramLMProbabilityMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {

    private final Logger logger = LoggerFactory.getLogger(BiGramLMProbabilityMapper.class);

    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        //logger.info("The key is: {} and value is:{} ",key,value);
        String biGramDetails = value.toString().split("\t")[1];
        String uniGramDetails = value.toString().split("\t")[2];
        long uniGramWordCount = Long.parseLong(uniGramDetails.split(" ")[1]);


        //Finding required probability
        if(biGramDetails.split(" ").length > 2){
            String[] bigGramWordDetails = biGramDetails.split(" ");
            String biGramWord = bigGramWordDetails[0] + " "+bigGramWordDetails[1];
            long biGramWordCount = Long.parseLong(bigGramWordDetails[2]);

            long totalBiGramWord = context.getConfiguration().getLong(NGramMain.CustomCounter.TOTAL_BI_GRAM_COUNT.name(), 0);
            long totalUniGramWord = context.getConfiguration().getLong(NGramMain.CustomCounter.TOTAL_UNI_GRAM_COUNT.name(), 0);

            double probabilityW1W2 = (double)biGramWordCount/totalBiGramWord;
            double probabilityW1 = (double)uniGramWordCount/totalUniGramWord;
            double finalProbability = probabilityW1W2/probabilityW1;
            context.write(new Text(biGramWord),new DoubleWritable(finalProbability));


        }

    }
}
