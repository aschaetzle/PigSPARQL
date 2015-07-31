package pigsparql.rdf;

/**
 * Abstract Parser Class for RDF Data.
 * 
 * Supported prefixes
 * ##################
 * general: foaf, xsd, dc, dcterms, dctype, rdf, rdfs, swrc, rss, owl, ex, rev, gn, gr, mo, og, sorg
 * SP2Bench: bench, person
 * LUBM: ub
 * BSBM: bsbm, bsbm-inst, bsbm-export
 * WatDiv: wsdbm
 * 
 * @author Alexander Schaetzle
 * @version 1.0
 */
public abstract class TripleParser {

	/**
	 * The predefined prefixes that are supported by the parser.
	 */
	protected static String[][] prefixes = new String[][] {
			{ "rdf:", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#" },
			{ "foaf:", "<http://xmlns.com/foaf/0.1/" },
			{ "foaf:", "<http://xmlns.com/foaf/" },
			{ "bench:", "<http://localhost/vocabulary/bench/" },
			{ "ub:", "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#" },
			{ "bsbm:", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/" },
			{ "bsbm-inst:", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/" },
			{ "bsbm-export:", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/" },
			{ "xsd:", "<http://www.w3.org/2001/XMLSchema#" },
			{ "dc:", "<http://purl.org/dc/elements/1.1/" },
			{ "dcterms:", "<http://purl.org/dc/terms/" },
			{ "dctype:", "<http://purl.org/dc/dcmitype/" },
			{ "rdfs:", "<http://www.w3.org/2000/01/rdf-schema#" },
			{ "swrc:", "<http://swrc.ontoware.org/ontology#" },
			{ "rev:", "<http://purl.org/stuff/rev#" },
			{ "rss:", "<http://purl.org/rss/1.0/" },
			{ "owl:", "<http://www.w3.org/2002/07/owl#" },
			{ "person:", "<http://localhost/persons/" },
			{ "ex:", "<http://example.org/" },
			{ "gn:", "<http://www.geonames.org/ontology#" },
			{ "gr:", "<http://purl.org/goodrelations/" },
			{ "mo:", "<http://purl.org/ontology/mo/" },
			{ "og:", "<http://ogp.me/ns#" },
			{ "sorg:", "<http://schema.org/" },
			{ "wsdbm:", "<http://db.uwaterloo.ca/~galuc/wsdbm/" }
	};

	/**
	 * Should the parser expand the prefixes in the Triple or not.
	 */
	protected boolean expand;
	protected boolean collapse;
	protected boolean canonicLiteral;
	protected String fieldDel;


	public TripleParser() {
		super();
		setPrefixMode(PrefixMode.NO_OP);
		setDelimiter(" ");
	}


	/**
	 * Parses an RDF Triple. Subject, Predicate and Object have to be separated
	 * with the defined delimiter. Returns a String array with 3 fields
	 * [subject, predicate, object] or null if the line could not be parsed.
	 * The user can set the delimiter with {@link #setDelimiter(String)} and they way prefixes are treated with {@link #setPrefixMode(PrefixMode)}.
	 * 
	 * @see <a href="http://www.w3.org/2001/sw/RDFCore/ntriples/">W3C N-Triples</a>
	 * 
	 * @param line
	 *            Input Line to parse
	 * @return String array [subject, predicate, object] or null
	 */
	public abstract String[] parseTriple(String line);


	/**
	 * Defines how to treat prefixes in URIs.
	 * Existing prefixes can be expanded ({@link PrefixMode#EXPAND}),
	 * known prefixes can be collapsed ({@link PrefixMode#COLLAPSE})
	 * or URI is used as it is ({@link PrefixMode#NO_OP}).
	 * Default is {@link PrefixMode#NO_OP}.
	 * 
	 * @param mode
	 */
	public void setPrefixMode(PrefixMode mode) {
		switch (mode) {
		case EXPAND:
			expand = true;
			collapse = false;
			break;
		case COLLAPSE:
			expand = false;
			collapse = true;
			break;
		case NO_OP:
			expand = false;
			collapse = false;
			break;
		}
	}


	/**
	 * Set delimiter used to split triple into subject, predicate, object.
	 * Default delimiter is whitespace.
	 * 
	 * @param delimiter
	 */
	public void setDelimiter(String delimiter) {
		fieldDel = delimiter;
	}


	/**
	 * Literals are replaced by a canonical representation without datatypes and whitespaces, if set to true.
	 * Default value is false.
	 * 
	 * @param value
	 */
	public void setCanonicalLiteral(boolean value) {
		canonicLiteral = value;
	}


	/**
	 * Splits the input into 3 fields (Subject, Predicate, Object).
	 * 
	 * @param input
	 *            input to be splitted
	 * @return Field-Array or null if input can't be splitted
	 */
	protected String[] splitTriple(String input) {
		// a Triple has 3 fields
		String[] fields = new String[3];

		// extract first field
		int delPos = input.indexOf(fieldDel);
		if (delPos == -1)
			return null;
		fields[0] = input.substring(0, delPos); // Subject
		if (fields[0] == null)
			return null;
		input = input.substring(delPos + 1);
		// delete leading delimiters (fields can be delimited by more than one
		// delimiter)
		while (input.startsWith(fieldDel)) {
			input = input.substring(1);
		}

		// extract second field
		delPos = input.indexOf(fieldDel);
		if (delPos == -1)
			return null;
		fields[1] = input.substring(0, delPos); // Predicate
		if (fields[1] == null)
			return null;
		input = input.substring(delPos + 1);
		// delete leading delimiters (fields can be delimited by more than one
		// delimiter)
		while (input.startsWith(fieldDel)) {
			input = input.substring(1);
		}

		// rest of input is the last field
		fields[2] = input; // Object

		return fields;
	}


	/**
	 * Checks if the the input is an URI. URIs are encapsulated in brackets (<>)
	 * or have a leading prefix. The prefix must be known to the Parser.
	 * 
	 * @param input
	 * @return true, if input is a URI
	 */
	protected boolean isURI(String input) {
		if (input.startsWith("<") && input.endsWith(">")) {
			return true;
		}
		// Prefix must be known
		else if (startsWithPrefix(input))
			return true;
		else
			return false;
	}


	/**
	 * Checks if the the input is a Blank Node. Blank Nodes have a leading _: .
	 * 
	 * @param input
	 * @return true, if input is a Blank Node
	 */
	protected boolean isBlankNode(String input) {
		if (input.startsWith("_:")) {
			return true;
		}
		else
			return false;
	}


	/**
	 * Checks if the input starts with a predefined prefix.
	 * 
	 * @param input
	 * @return true, is input starts with a predefined prefix
	 */
	protected boolean startsWithPrefix(String input) {
		for (int i = 0; i < prefixes.length; i++) {
			if (input.startsWith(prefixes[i][0]))
				return true;
		}
		return false;
	}


	/**
	 * Expands predefined leading Prefixes if expand is set true.
	 * If expand is not set true it just returns the input.
	 * 
	 * @param input
	 * @return input with expanded Prefix if expand is set true
	 */
	protected String expandPrefix(String input) {
		if (!expand)
			return input;
		// URIs without a prefix or Blank Nodes don't have to be treated
		if (input.startsWith("<") && input.endsWith(">")) {
			return input;
		}
		if (input.startsWith("_:")) {
			return input;
		}
		// check if one of the predefined prefixes matches
		for (int i = 0; i < prefixes.length; i++) {
			if (input.contains(prefixes[i][0])) {
				// replace prefix if a match is found
				input = input.replace(prefixes[i][0], prefixes[i][1]);
				// close URI
				return input + ">";
			}
		}
		return input;
	}


	/**
	 * Collapses predefined leading Prefixes if collapse is set true.
	 * If collapse is not set true it just returns the input.
	 * 
	 * @param input
	 * @return input with collapsed Prefix if collapse is set true
	 */
	protected String collapsePrefix(String input) {
		if (!collapse)
			return input;
		// check if one of the predefined prefixes matches
		for (int i = 0; i < prefixes.length; i++) {
			if (input.contains(prefixes[i][1])) {
				// replace prefix with abbreviation if a match is found
				input = input.replace(prefixes[i][1], prefixes[i][0]);
				// remove closing bracket of URI
				return input.substring(0, input.length() - 1);
			}
		}
		return input;
	}


	/**
	 * Converts a literal to its canonical representation without datatype and whitespaces.
	 * 
	 * @param literal
	 * @return
	 */
	protected String toCanonicalLiteral(String literal) {
		// if it is not a literal, simply return it
		if (!literal.startsWith("\"")) {
			return literal;
		}
		// extract the literal out of the (typed) literal string
		int endOfLiteral = literal.lastIndexOf("\"");
		literal = literal.substring(1, endOfLiteral);
		literal = literal.replace(" ", "_");
		return literal;
	}

}