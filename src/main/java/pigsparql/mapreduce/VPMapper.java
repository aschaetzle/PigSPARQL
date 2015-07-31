package pigsparql.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pigsparql.rdf.PrefixMode;
import pigsparql.rdf.SimpleTripleParser;
import pigsparql.rdf.TripleParser;


public class VPMapper extends Mapper<LongWritable, Text, Text, TextPair> {

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
		// Set parameters parsed from commandline
		Configuration conf = context.getConfiguration();
		delimiter = conf.get("rdf.delimiter");
		expand = conf.getBoolean("rdf.expand", false);
		collapse = conf.getBoolean("rdf.collapse", false);
		canonicLiteral = conf.getBoolean("rdf.literal", false);
		// instantiate RDFParser as specified in property 'rdf.parser'
		// use SimpleTripleParser as fallback
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
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] parsedTriple = rdfParser.parseTriple(value.toString());
		if (parsedTriple != null) {
			// Convert liters to Pig Types, if possible
			parsedTriple[2] = Util.toPigTypes(parsedTriple[2]);
			// Use Predicate for Vertical Partitioning
			context.write(new Text(parsedTriple[1]), new TextPair(parsedTriple[0], parsedTriple[2]));
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
