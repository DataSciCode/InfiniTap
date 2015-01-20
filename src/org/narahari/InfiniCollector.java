package org.narahari;

 

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;

import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;

import org.apache.hadoop.mapred.*;
import org.infinispan.hadoopintegration.mapreduce.output.InfinispanOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//Class InfiniCollector is a kind of cascading.tuple.TupleEntrySchemeCollector 
//that writes tuples to the resource managed by a particular InfiniTap instance.

/**
 * @author Srinivas Narahari
 *Jan 19, 2015
 */
public class InfiniCollector extends TupleEntrySchemeCollector implements OutputCollector {

	
	
	
	 private static final Logger LOG = LoggerFactory.getLogger(InfiniCollector.class);
	  private final JobConf conf;
	  private RecordWriter writer;
	  private final FlowProcess<JobConf> hadoopFlowProcess;
	  private final Tap<JobConf, RecordReader, OutputCollector> tap;
	  private final Reporter reporter = Reporter.NULL;
	
	public InfiniCollector(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap) throws IOException {
    super(flowProcess, tap.getScheme());
    this.hadoopFlowProcess = flowProcess;
    this.tap = tap;
    this.conf = new JobConf(flowProcess.getConfigCopy());
    this.setOutput(this);
	}


	  @Override
	  public void prepare() {
	    try {
	      initialize();
	    } catch (IOException e) {
	      throw new RuntimeException(e);
	    }

	    super.prepare();
	  }
	  

	  private void initialize() throws IOException {
	    tap.sinkConfInit(hadoopFlowProcess, conf);
	    InfinispanOutputFormat outputFormat = (InfinispanOutputFormat) conf.getOutputFormat();
	    writer = outputFormat.getRecordWriter(null, conf, tap.getIdentifier(), Reporter.NULL);
	    sinkCall.setOutput(this);
	  }

	  @Override
	  public void close() {
	    try {
	      writer.close(reporter);
	    } catch (IOException exception) {
	      LOG.error("Exception at InfiniCollector.close(): ", exception);
	      throw new TapException("Exception closing InfiniCollector", exception);
	    } finally {
	      super.close();
	    }
	  }
	@Override
	public void collect(Object writableComparable, Object writable) throws IOException {
		
		LOG.info("Collect mode" + writable);
	    if (hadoopFlowProcess instanceof HadoopFlowProcess)
	        ((HadoopFlowProcess) hadoopFlowProcess).getReporter().progress();
	      writer.write(writableComparable, writable);
	      
	      
	    }
	

 

}
