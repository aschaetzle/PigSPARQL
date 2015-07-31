package pigsparql.rdf;

/**
 * Simple Parser for RDF Data in Triples format. There is no type checking
 * applied, the input line is just parsed to a Triple using the given delimiter.
 * 
 * @author Alexander Schaetzle
 * @version 1.0
 */
public class SimpleTripleParser extends TripleParser {
	
	public SimpleTripleParser() {
		super();
	}

	/**
	 * Parses an RDF Triple. Subject, Predicate and Object have to be separated
	 * with the given delimiter. Returns a String array with 3 fields [subject,
	 * predicate, object] or null if the line could not be parsed. The user can
	 * decide whether the prefixes should be expanded or not.
	 * 
	 * @see <a href="http://www.w3.org/2001/sw/RDFCore/ntriples/">W3C N-Triples</a>
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

		// if line ends with a dot, remove it
		if (line.endsWith(".")) {
			// remove trailing dot and discard possible whitespaces
			line = line.substring(0, line.length() - 1).trim();
		}

		// if line starts with '@', ignore it
		if (line.startsWith("@"))
			return null;

		// split input line into 3 fields using the given delimiter
		String[] triple = splitTriple(line);
		if (triple == null)
			return null;
		if (expand) {
			triple[0] = expandPrefix(triple[0]);
			triple[1] = expandPrefix(triple[1]);
			triple[2] = expandPrefix(triple[2]);
		}
		else if (collapse) {
			triple[0] = collapsePrefix(triple[0]);
			triple[1] = collapsePrefix(triple[1]);
			triple[2] = collapsePrefix(triple[2]);
		}
		if (canonicLiteral) {
			triple[2] = toCanonicalLiteral(triple[2]);
		}
		return triple;
	}

}
