package org.narahari;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.infinispan.hadoopintegration.mapreduce.input.InfinispanInputFormat;
import org.infinispan.hadoopintegration.mapreduce.output.InfinispanOutputFormat;
import org.infinispan.hadoopintegration.mapreduce.output.InfinispanRecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

 
/**
 * @author Srinivas Narahari
 *Jan 19, 2015
 */
public class InfiniScheme extends
		Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

	private Fields infiniFields = new Fields("key", "value");

	private static final Logger LOG = LoggerFactory
			.getLogger(InfiniScheme.class);

	protected String schemeUUID;

	enum TapType {

		SOURCE, SINK
	};

	public InfiniScheme() {
		setSourceFields(infiniFields);
		setSinkFields(infiniFields);
		initializeUUID();
	}

	private void initializeUUID() {
		this.schemeUUID = UUID.randomUUID().toString();

	}

	// @Override method sourcePrepare is used to initialize resources needed
	// during each call of source(cascading.flow.FlowProcess, SourceCall).
	// Place any initialized objects in the SourceContext so each instance will
	// remain threadsafe.
	@Override
	public void sourcePrepare(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) {
		Object[] pair = new Object[] { sourceCall.getInput().createKey(),
				sourceCall.getInput().createValue() };
		sourceCall.setContext(pair);
	}

	// @Override method sourceCleanup is used to destroy resources created by
	// sourcePrepare(cascading.flow.FlowProcess, SourceCall).
	@Override
	public void sourceCleanup(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) {
		sourceCall.setContext(null);
	}

	// Method setFields sets the sourceFields & sinkFields of this Scheme
	// object.
	private void setFields(TapType tapType) {
		if (tapType == TapType.SOURCE) {
			setSourceFields(infiniFields);
		} else {
			setSinkFields(infiniFields);
		}

	}

	@Override
	public void sink(FlowProcess<JobConf> flowProcess,
			SinkCall<Object[], OutputCollector> sinkCall) throws IOException {

		TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
		OutputCollector outputCollector = sinkCall.getOutput();

		Tuple tuple = tupleEntry.selectTuple(this.infiniFields);
		// LOG.info("inside sink with value" + sinkCall.getContext()[ 1 ]
		// +"and Key " + sinkCall.getContext()[0] );

		// outputCollector.collect( sinkCall.getContext()[0],
		// sinkCall.getContext()[ 1 ]);

		// outputCollector.collect(null, getMutations(tuple));

		// outputCollector.collect(null, put);

		Tuple result = getSinkFields() != null ? tupleEntry
				.selectTuple(getSinkFields()) : tupleEntry.getTuple();
		// outputCollector.collect(NullWritable.get(), result);
		outputCollector.collect(objToText(tuple.get(0)),
				objToText(tuple.get(1)));
	}

	private static Text objToText(Object o) {
		return new Text(objToBytes(o));
	}

	private static byte[] objToBytes(Object o) {
		if (o instanceof String) {
			String str = (String) o;
			return str.getBytes();
		} else if (o instanceof Long) {
			Long l = (Long) o;
			return l.toString().getBytes();
		} else if (o instanceof Integer) {
			Integer l = (Integer) o;
			return l.toString().getBytes();
		} else if (o instanceof Boolean) {
			Boolean l = (Boolean) o;
			return l.toString().getBytes();
		} else if (o instanceof Float) {
			Float l = (Float) o;
			return l.toString().getBytes();
		} else if (o instanceof Double) {
			Double l = (Double) o;
			return l.toString().getBytes();
		} else if (o instanceof Text) {
			Text l = (Text) o;
			return l.toString().getBytes();
		}

		return new String("").getBytes();
	}

	// @Override method Method sinkInit initializes this instance as a sink.
	// This method is executed client side as a means to provide necessary
	// configuration
	// parameters used by the underlying platform.
	@Override
	public void sinkConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

		conf.setOutputKeyClass(IntWritable.class); // be explicit

		conf.setOutputValueClass(InfinispanRecordWriter.class); // be explicit
		conf.setOutputFormat(InfinispanOutputFormat.class);

		setFields(TapType.SINK);

		InfiniTap infiTap = (InfiniTap) tap;

		String commaSeparated = tap.getIdentifier();

		ArrayList items = new ArrayList(
				Arrays.asList(commaSeparated.split(",")));
		conf.set("mapreduce.ispn.inputsplit.remote.cache.host", items.get(0)
				.toString()); // "localhost"); // items.get(0));
		conf.set("mapreduce.ispn.input.remote.cache.host", items.get(0)
				.toString()); // "localhost");
		conf.set("mapreduce.ispn.output.remote.cache.host", items.get(0)
				.toString()); // "localhost");

		conf.set("mapreduce.ispn.inputsplit.remote.cache.port",
				(String) items.get(1));// "11222");
		conf.set("mapreduce.ispn.input.remote.cache.port",
				(String) items.get(1));// "11222");
		conf.set("mapreduce.ispn.output.remote.cache.port",
				(String) items.get(1));// "11222");

		conf.set("mapreduce.ispn.input.cache.name", items.get(2).toString());// "map-reduce-in"
																				// );
		conf.set("mapreduce.ispn.output.cache.name", items.get(3).toString()); // "map-reduce-out"
																				// );

		LOG.info("cachename " + items.get(2).toString());

		LOG.info("Inside SinkConf init  in Scheme");
		LOG.info("Scheme Conf " + tap.getIdentifier());

		conf.set("mapreduce.ispn.input.converter",
				InputConverter.class.getCanonicalName());
		conf.set("mapreduce.ispn.output.converter",
				OutputConverter.class.getCanonicalName());
		// conf.setInputFormat(KeyValueTextInputFormat.class);

	}

	@Override
	// Method source will read a new "record" or value from
	// SourceCall.getInput() and populate
	// the available Tuple via SourceCall.getIncomingEntry() and return true on
	// success or false
	// if no more values available.
	public boolean source(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) {

		try {

			// String rowKey = (String) sourceCall.getContext()[0];
			// LOG.info("source Key and value are " + sourceCall.getContext()[0]
			// + sourceCall.getContext()[ 1 ]);
			Object key = sourceCall.getContext()[0];
			Object value = sourceCall.getContext()[1];

			boolean hasNext = sourceCall.getInput().next(key, value);
			if (!hasNext)
				return false;

			Tuple resultTuple = new Tuple();
			resultTuple.add(key);
			resultTuple.add(value);

			sourceCall.getIncomingEntry().setTuple(resultTuple);

			return true;

		} catch (Exception e) {
			LOG.error("Error in AccumuloScheme.source", e);
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public void sourceConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

		conf.setInputFormat(InfinispanInputFormat.class);

		setFields(TapType.SOURCE);

		InfiniTap infiTap = (InfiniTap) tap;

		String commaSeparated = tap.getIdentifier();

		ArrayList items = new ArrayList(
				Arrays.asList(commaSeparated.split(",")));
		conf.set("mapreduce.ispn.inputsplit.remote.cache.host", items.get(0)
				.toString()); // "localhost"); // items.get(0));
		conf.set("mapreduce.ispn.input.remote.cache.host", items.get(0)
				.toString()); // "localhost");
		conf.set("mapreduce.ispn.output.remote.cache.host", items.get(0)
				.toString()); // "localhost");

		conf.set("mapreduce.ispn.inputsplit.remote.cache.port",
				(String) items.get(1));// "11222");
		conf.set("mapreduce.ispn.input.remote.cache.port",
				(String) items.get(1));// "11222");
		conf.set("mapreduce.ispn.output.remote.cache.port",
				(String) items.get(1));// "11222");

		conf.set("mapreduce.ispn.input.cache.name", items.get(2).toString());// "map-reduce-in"
																				// );
		conf.set("mapreduce.ispn.output.cache.name", items.get(3).toString()); // "map-reduce-out"
																				// );

		LOG.info("Inside SourceConfi init  in Schem");
		LOG.info("Scheme Conf " + tap.getIdentifier());

		conf.set("mapreduce.ispn.input.converter",
				InputConverter.class.getCanonicalName());
		conf.set("mapreduce.ispn.output.converter",
				OutputConverter.class.getCanonicalName());
		// conf.setInputFormat(KeyValueTextInputFormat.class);

	}

}
