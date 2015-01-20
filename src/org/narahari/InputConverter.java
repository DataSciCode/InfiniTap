package org.narahari;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.infinispan.hadoopintegration.mapreduce.input.InfinispanInputConverter;
 

/*


public class InputConverter implements InfinispanInputConverter<LongWritable, Text, Integer, String> {
    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public void setKey(LongWritable longWritable, Integer integer) {
        longWritable.set(integer);
    }

    @Override
    public void setValue(Text text, String s) {
        text.set(s);
    }
} */



/**
 * @author Srinivas Narahari
 *Jan 19, 2015
 */
public class InputConverter implements InfinispanInputConverter<Text, Text, String, String> {

	@Override
	public Text createKey() {
	  return new Text();
	  
	}

	@Override
	public Text createValue() {
		  return new Text();
		  
	}

	@Override
	public void setKey(Text key, String key2) {
		 key.set(key2);
		
	}

	@Override
	public void setValue(Text value, String value2) {
	  value.set(value2);
	}

	 
 

}