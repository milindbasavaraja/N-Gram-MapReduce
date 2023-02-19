# N-Gram-Map-Reducer


The jar is present in out/artifacts/N_Gram_MapReduce_jar

**Command to Run Jar**

> hadoop jar N-Gram-MapReduce.jar NGramMain mb7949-bd23/hw1dir mb7949-bd23/hw1.2/output-bi-uni-gram-1 mb7949-bd23/hw1.2/output-clean-1 mb7949-bd23/hw1.2/output-probability-1

**Explanation of the Command**

**NGramMain** is the driver class which contains Main method.

**mb7949-bd23/hw1dir** is the first argument, which is input files/directory to the program.

**mb7949-bd23/hw1.2/output-bi-uni-gram-1** is the second argument, which is output of first Map-Reducer job.

**mb7949-bd23/hw1.2/output-clean-1** is the third argument, which is output of second Map-Reducer job.

**mb7949-bd23/hw1.2/output-probability-1** is the third argument, which is the final output.



### MapReducer Jobs Explanation

1. In first MapReducer job I calculate individual UniGram and BiGram count along with **TOTAL_UNI_GRAM** and **TOTAL_BI_GRAM** count.

2. In Second MapReducer job, I create a custom **WritableComparable** which will be used as a composite key in my Mapper and Reducer. The composite key is used for Secondary Sorting. Here, I am using custom Partitioner, custom Sorter and custom Group Sorter.

3. In third Mapper job, I calculate the probability of **P(w2|w1)**.
