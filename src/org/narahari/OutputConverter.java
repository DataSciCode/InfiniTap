package org.narahari;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.infinispan.hadoopintegration.mapreduce.output.InfinispanOutputConverter;

 
/*


public class OutputConverter implements InfinispanOutputConverter<Text, IntWritable, String, Integer> {

    @Override
    public String convertKey(Text text) {
        return text.toString();
    }

    @Override
    public Integer convertValue(IntWritable intWritable) {
        return intWritable.get();
    }
}
*/

/**
 * @author Srinivas Narahari
 *Jan 19, 2015
 */
public class OutputConverter implements InfinispanOutputConverter<Text, Text, String, String> {

    @Override
    public String convertKey(Text text) {
        return text.toString();
    }

    @Override
    public String convertValue(Text text) {
    	 return text.toString();
    }
}