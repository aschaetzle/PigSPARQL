package pigsparql.mapreduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;


// Writable for Arrays of Type Text
public class TextArrayWritable extends ArrayWritable {
	public TextArrayWritable() {
		super(Text.class);
	}
}
