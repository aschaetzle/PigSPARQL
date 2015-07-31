package pigsparql.rdf;

/**
 * Parser for RDF Data encoded as extended N-Triple. The parser takes a String
 * with a RDF Triple in N-Triple syntax as input and returns the subject,
 * predicate and object of the Triple in an array. Beyond the syntax of
 * N-Triples the parser also supports the usage of prefixes. The user can decide
 * whether to expand the prefixes in the returned Triple or not. The used
 * prefixes should be in the list of supported prefixes, otherwise parsing of
 * the corresponding Triple will fail. The parser also supports Blank Nodes and
 * typed Literals.
 * 
 * @author Alexander Schaetzle
 * @version 1.2
 */
public class ExNTriplesParser extends TripleParser {

	public ExNTriplesParser() {
		super();
	}


	/**
	 * Parses an RDF Triple in extended N-Triple syntax given as String. Subject,
	 * Predicate and Object have to be separated with the given delimiter.
	 * Subjects can be URIs or Blank Nodes, Predicates must be URIs and Objects
	 * can be URIs, Blank Nodes or Typed Literales. Returns a String array with
	 * 3 fields [subject, predicate, object] or null if the line could not be
	 * parsed. The user can decide whether the prefixes should be expanded or
	 * not.
	 * 
	 * @see <a href="http://www.w3.org/2001/sw/RDFCore/ntriples/">W3C N-Triples</a>
	 * @see <a href="http://www.w3.org/TR/rdf-concepts/#section-Literals">RDF Concepts: Literals</a>
	 * @see <a href="http://www.w3.org/TR/rdf-concepts/#section-Graph-Literal">RDF Abstract Syntax: Literals</a>
	 * 
	 * @param line
	 *            Input Line to parse
	 * @param _expand
	 *            expand prefixes or not
	 * @return String array [subject, predicate, object] or null
	 */
	@Override
	public String[] parseTriple(String line) {
		// first, discard leading and trailing whitespaces
		line = line.trim();

		// line has to end with a dot, otherwise it is a syntax error
		if (line.endsWith(".")) {
			// remove trailing dot and discard possible whitespaces
			line = line.substring(0, line.length() - 1).trim();
		}
		else
			return null;

		// split input line into 3 fields using the given delimiter
		String[] fields = splitTriple(line);
		if (fields == null)
			return null;

		/*
		 * Parse subject, predicate and object. If any of these can't be parsed
		 * a null value is returned. When parsing fails there is a syntax error
		 * in the corresponding field.
		 */
		String subject = parseSubject(fields[0]);
		if (subject == null)
			return null;
		String predicate = parsePredicate(fields[1]);
		if (predicate == null)
			return null;
		String object = parseObject(fields[2]);
		if (object == null)
			return null;

		if (canonicLiteral)
			object = toCanonicalLiteral(object);
		// return the parsed Triple
		return new String[] { subject, predicate, object };
	}


	/**
	 * Parses the Subject of the Triple. Subjects can be URIs or Blank Nodes.
	 * 
	 * @param input
	 *            Subject to be parsed
	 * @return parsed Subject or null (syntax error)
	 */
	private String parseSubject(String input) {
		// Subject can be an URI
		if (isURI(input)) {
			if (expand)
				return expandPrefix(input);
			else if (collapse)
				return collapsePrefix(input);
			else
				return input;
		}
		// Subject can be a Blank Node
		else if (isBlankNode(input))
			return input;
		// If Subject is neither an URI nor Blank Node, it is not valid RDF syntax
		else
			return null;
	}


	/**
	 * Parses the Predicate of the Triple. Predicates must be URIs.
	 * 
	 * @param input
	 *            Predicate to be parsed
	 * @return parsed Predicate or null (syntax error)
	 */
	private String parsePredicate(String input) {
		// Predicate must be an URI
		if (isURI(input)) {
			if (expand)
				return expandPrefix(input);
			else if (collapse)
				return collapsePrefix(input);
			else
				return input;
		}
		// If Predicate is not an URI, it is not valid RDF syntax
		else
			return null;
	}


	/**
	 * Parses the Object of the Triple. Objects can be URIs, Blank Nodes or
	 * Literals (plain or typed).
	 * 
	 * @param input
	 *            Object to be parsed
	 * @return parsed Object or null (syntax error)
	 */
	private String parseObject(String input) {
		if (isURI(input)) {
			if (expand)
				return expandPrefix(input);
			else if (collapse)
				return collapsePrefix(input);
			else
				return input;
		}
		// Object can be a Blank Node
		else if (isBlankNode(input))
			return input;
		// If Object is not an URI or Blank Node it must be a Literal
		else
			return parseLiteralObject(input);
	}


	/**
	 * Parses a Literal Object. Literal Objects can be plain or typed.
	 * 
	 * @param input
	 *            Literal Object to be parsed
	 * @return parsed Literal or null (syntax error)
	 */
	private String parseLiteralObject(String input) {
		// Check whether it is a number or a plain literal
		if (input.matches("[0-9]+") || input.matches("[0-9]+\\.[0-9]+") || input.matches("\"(.)*\"")) {
			return input;
		}
		// If it is not a number nor a plain literal it has to be a typed literal
		else
			return parseTypedLiteralObject(input);
	}


	/**
	 * Parses a Typed Literal Object. Typed Literals can have a Language Tag (@)
	 * or a Datatype (^^)
	 * 
	 * @param input
	 *            Typed Literal to be parsed
	 * @return parsed Typed Literal or null (syntax error)
	 */
	private String parseTypedLiteralObject(String input) {
		// typed literals have to start with quotation mark (")
		if (!input.startsWith("\"")) {
			return null; // syntax error
		}
		// extract the literal out of the typed literal
		int endOfLiteral = input.lastIndexOf("\"") + 1;
		String literal = input.substring(0, endOfLiteral);
		// empty literals ("") are not allowed
		if (literal.length() <= 2) {
			return null; // syntax error
		}
		// check if it has a language tag
		else if (input.substring(endOfLiteral, endOfLiteral + 1).equals("@")) {
			/*
			 * extract the language tag out of the typed literal, normalize the
			 * language tag and return the normalized typed literal
			 */
			String languageTag = input.substring(endOfLiteral + 1, input.length());
			languageTag = languageTag.toLowerCase();
			return literal + "@" + languageTag;
		}
		// check if it has a datatype
		else if (input.substring(endOfLiteral, endOfLiteral + 2).equals("^^")) {
			/*
			 * extract the datatype out of the typed literal, expand prefixes if
			 * wanted and return the typed literal
			 */
			String datatype = input.substring(endOfLiteral + 2, input.length());
			if (expand)
				return literal + "^^" + expandPrefix(datatype);
			else if (collapse)
				return literal + "^^" + collapsePrefix(datatype);
			else
				return literal + "^^" + datatype;
		}
		// if the typed literal has neither a language tag nor a datatype it is
		// a syntax error
		else
			return null;
	}

}
