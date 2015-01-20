package org.narahari;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import javax.annotation.concurrent.Immutable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
 
 

import org.infinispan.hadoopintegration.mapreduce.input.InfinispanInputConverter;
//import org.infinispan.hadoopintegration.mapreduce.input.InfinispanInputConverter;
import org.infinispan.hadoopintegration.mapreduce.input.InfinispanInputFormat;
import org.infinispan.hadoopintegration.mapreduce.output.InfinispanOutputConverter;
//import org.infinispan.hadoopintegration.mapreduce.output.InfinispanOutputConverter;
import org.infinispan.hadoopintegration.mapreduce.output.InfinispanOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

 

/**
 * @author Srinivas Narahari
 *Jan 19, 2015
 */
public class InfiniTap extends Tap<JobConf, RecordReader, OutputCollector>
		implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(InfiniTap.class);
	private String tapUUID;
	 private Fields infiniFields = new Fields("key", "value");
	
    private Configuration conf;
	private String resourceID;
    // Constructor
    public InfiniTap(String infiConnectionString,
            InfiniScheme infiScheme) throws Exception {

        super(infiScheme, SinkMode.REPLACE);
        setResourceFromConnectionString(infiConnectionString);
    }

    // Constructor
    public InfiniTap(String infiniConnectionString,
            InfiniScheme infiniScheme, SinkMode sinkMode) throws Exception {

        super(infiniScheme, sinkMode);
        setResourceFromConnectionString(infiniConnectionString);

    }
 // Method setResourceFromConnectionString initializes Infinispan related
  	// data
  	// members 	

	private void setResourceFromConnectionString(String infiniConnectionString) throws IOException {
		// TODO Auto-generated method stub
	 	 try {
	 		   this.tapUUID = UUID.randomUUID().toString();
	 			// neeed to set connection... heree...

	 			   LOG.info("Creating config " + infiniConnectionString );
	 		    	String commaSeparated =  infiniConnectionString;
	 		 
	 			
	 			
	 	 
	 			 ArrayList items= new ArrayList(Arrays.asList(commaSeparated.split(",")));
	 			
	 		 
	 		     Configuration conf = new Configuration();
	 			
 
	 		     
	 		     
	 		    conf.set("mapreduce.ispn.inputsplit.remote.cache.host",items.get(0).toString()) ; // "localhost"); // items.get(0));
	 	        conf.set("mapreduce.ispn.input.remote.cache.host",items.get(0).toString()); //"localhost");
	 	        conf.set("mapreduce.ispn.output.remote.cache.host",items.get(0).toString()); //"localhost");
	 	        
	 	        conf.set("mapreduce.ispn.inputsplit.remote.cache.port",(String) items.get(1));// "11222");
	 	        conf.set("mapreduce.ispn.input.remote.cache.port",(String) items.get(1));//"11222");
	 	        conf.set("mapreduce.ispn.output.remote.cache.port",(String) items.get(1));//"11222");
	 	        
	 			conf.set("mapreduce.ispn.input.cache.name", items.get(2).toString());//"map-reduce-in" );
	 			conf.set("mapreduce.ispn.output.cache.name",items.get(3).toString()); //"map-reduce-out"  );
	 
	 
	 			   
   conf.set("mapreduce.ispn.input.converter", InputConverter.class.getCanonicalName());
  conf.set("mapreduce.ispn.output.converter",OutputConverter.class.getCanonicalName());
	 			
//		conf.set("mapreduce.ispn.input.converter", InfinispanInputConverter.class.getCanonicalName());
// 	    conf.set("mapreduce.ispn.output.converter",InfinispanOutputConverter.class.getCanonicalName());
//
	 			JobConf jobConf = new JobConf(conf, InfiniTap.class);
	 			jobConf.setJarByClass(InfiniTap.class);
	 			jobConf.setJobName("cascadingTap");
	 

	 
	 		//    jobConf.setOutputKeyClass(Text.class);
	 	    //    jobConf.setOutputValueClass(IntWritable.class);

	 	     //   jobConf.setMapOutputKeyClass(Text.class);
	 	    //    jobConf.setMapOutputValueClass(IntWritable.class);
	 	        
	 	        
	 	        jobConf.setInputFormat(InfinispanInputFormat.class);
	 	        jobConf.setOutputFormat(InfinispanOutputFormat.class);
	 	       
	 
	 	        
	 			
	 		//	this.tapUUID = infiniConnectionString;
	 			this.resourceID = infiniConnectionString;

	 	 	} catch (Exception e) {
	  	       throw new IOException(
	  				"Bad parameter; Format expected is: host,port,cachein,cacheout"); }
	 	 	}
	  

	
    public Fields getDefaultInfiniFields() {
        return infiniFields;
    }


    
	@Override
	public String getIdentifier() {
		// TODO Auto-generated method stub
		   return this.resourceID;
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess,
			RecordReader input) throws IOException {
		return new HadoopTupleEntrySchemeIterator(flowProcess, this,
				input);
	}
	 

	@Override
	public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess,
			OutputCollector output) throws IOException {
		InfiniCollector infiCollector = new InfiniCollector(flowProcess, this);
		infiCollector.prepare();
		return infiCollector;
		
	}

	@Override
	public boolean createResource(JobConf conf) throws IOException {
		 
		  throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public boolean deleteResource(JobConf conf) throws IOException {
		  throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public boolean resourceExists(JobConf conf) throws IOException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public long getModifiedTime(JobConf conf) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

    @Override
    public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
        super.sinkConfInit(process, conf);
    }

	
}
