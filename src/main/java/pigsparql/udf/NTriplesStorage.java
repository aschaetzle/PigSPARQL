package pigsparql.udf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.pig.LoadCaster;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import pigsparql.rdf.PrefixMode;
import pigsparql.rdf.SimpleTripleParser;
import pigsparql.rdf.TripleParser;


public class NTriplesStorage extends PigStorage {

	private TupleFactory mTupleFactory = TupleFactory.getInstance();
	private TripleParser rdfParser;


	/**
	 * Default Constructor of class NTriplesStorage.
	 * If no arguments are passed prefixes are not expanded or collapsed by default.
	 */
	public NTriplesStorage() {
		this(" ", "");
	}


	public NTriplesStorage(String delimiter) {
		this(delimiter, "");
	}


	/**
	 * Constructor of class NTriplesStorage.
	 * The user can pass the option String 'expand' or 'collapse' to the constructor to indicate
	 * that prefixes should be expanded or collapsed when loading a Triple.
	 * 
	 * @param options
	 *            'expand' or 'collapse'
	 */
	public NTriplesStorage(String delimiter, String options) {
		super(delimiter);
		rdfParser = new SimpleTripleParser();
		rdfParser.setDelimiter(delimiter);

		switch (options) {
		case "expand":
			rdfParser.setPrefixMode(PrefixMode.EXPAND);
			break;
		case "collapse":
			rdfParser.setPrefixMode(PrefixMode.COLLAPSE);
			break;
		default:
			rdfParser.setPrefixMode(PrefixMode.NO_OP);
		}
	}


	@Override
	public Tuple getNext() throws IOException {
		try {
			boolean notDone = in.nextKeyValue();
			if (!notDone) {
				return null;
			}
			Text value = (Text) in.getCurrentValue();
			// Use the Parser to parse the input line into a Triple
			String[] triple = rdfParser.parseTriple(value.toString());
			// If the parser returns null this line was not a valid RDF triple
			// We then continue with the next line
			if (triple == null) {
				mLog.warn("This is not an RDF triple -> ignored: " + value.toString());
				return getNext();
			}
			// Tuples are always triples of Subject, Predicate, Object
			Tuple t = mTupleFactory.newTuple(3);
			// set the fields of the Tuple as Subject, Predicate, Object
			t.set(0, new DataByteArray(triple[0])); // Subject
			t.set(1, new DataByteArray(triple[1])); // Predicate
			t.set(2, new DataByteArray(triple[2])); // Object
			return t;
		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}
	}


	@Override
	public LoadCaster getLoadCaster() throws IOException {
		return new RDFStorageConverter();
	}

}
