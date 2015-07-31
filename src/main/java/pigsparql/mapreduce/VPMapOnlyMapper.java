package pigsparql.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import pigsparql.rdf.PrefixMode;
import pigsparql.rdf.SimpleTripleParser;
import pigsparql.rdf.TripleParser;


public class VPMapOnlyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	private Class<TripleParser> rdfParserClass;
	private TripleParser rdfParser;

	private String delimiter;
	private boolean expand;
	private boolean collapse;
	private boolean canonicLiteral;

	private static final String VALID_TRIPLES = "Valid RDF triples";
	private static final String INVALID_TRIPLES = "Invalid RDF triples";
	private static final String IGNORED_LINES = "Ignored input lines";


	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// Use MultipleOutputs for Vertical Partitioning
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		// Set parameters parsed from commandline
		Configuration conf = context.getConfiguration();
		delimiter = conf.get("rdf.delimiter");
		expand = conf.getBoolean("rdf.expand", false);
		collapse = conf.getBoolean("rdf.collapse", false);
		canonicLiteral = conf.getBoolean("rdf.literal", false);
		// Instantiate RDFParser as specified in property 'rdf.parser'
		// Use SimpleTripleParser as fallback
		rdfParserClass = (Class<TripleParser>) conf.getClass("rdf.parser", SimpleTripleParser.class);
		try {
			rdfParser = rdfParserClass.newInstance();
			rdfParser.setCanonicalLiteral(canonicLiteral);
			rdfParser.setDelimiter(delimiter);
			if (expand)
				rdfParser.setPrefixMode(PrefixMode.EXPAND);
			else if (collapse)
				rdfParser.setPrefixMode(PrefixMode.COLLAPSE);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		System.out.println("Using RDF Parser " + rdfParserClass.getName());
	}


	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] parsedTriple = rdfParser.parseTriple(value.toString());
		if (parsedTriple != null) {
			// Convert liters to Pig Types, if possible
			parsedTriple[2] = Util.toPigTypes(parsedTriple[2]);
			// Use Predicate for Vertical Partitioning
			multipleOutputs.write(NullWritable.get(), new Text(parsedTriple[0] + "\t" + parsedTriple[2]),
					Util.generateFileName(parsedTriple[1]));
			// Write all parsed triples also to "inputData" for queries where Predicate is not known
			multipleOutputs.write(NullWritable.get(), new Text(parsedTriple[0] + "\t" + parsedTriple[1] + "\t" + parsedTriple[2]),
					Util.generateFileName("inputData"));
			context.getCounter("RDF Dataset Properties", VALID_TRIPLES).increment(1);
		} else {
			if (value.getLength() == 0 || value.toString().startsWith("@")) {
				System.out.println("IGNORING: " + value);
				context.getCounter("RDF Dataset Properties", IGNORED_LINES).increment(1);
			} else {
				System.out.println("DISCARDED: " + value);
				context.getCounter("RDF Dataset Properties", INVALID_TRIPLES).increment(1);
			}
		}
	}

}
