package pigsparql.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class VPReducer extends Reducer<Text, TextPair, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> multipleOutputs;


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// use MultipleOutputs for Vertical Partitioning
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}


	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}


	@Override
	public void reduce(Text key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
		for (TextPair value : values) {
			// Use key (Predicate) as folder name for Vertical Partitioning
			multipleOutputs.write(NullWritable.get(), new Text(value.getFirst() + "\t" + value.getSecond()),
					Util.generateFileName(key.toString()));
			// Write all parsed triples also to "inputData" for queries where Predicate is not known
			multipleOutputs.write(NullWritable.get(), new Text(value.getFirst() + "\t" + key.toString() + "\t" + value.getSecond()),
					Util.generateFileName("inputData"));
		}

	}

}
