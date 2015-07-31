package pigsparql.mapreduce;

public final class Util {

	// Suppress default constructor for non-instantiability
	private Util() {
	}


	/**
	 * Converts given String into a String that can be used in a Filename.
	 * 
	 * @param name
	 * @return converted input String
	 */
	public static String convertToFSName(String name) {
		return name.replace(':', '_').replace('#', '_').replace('~', '_').replace('/', '_');
	}


	/**
	 * Generates an HDFS compliant Filename.
	 * 
	 * @param name
	 * @return filename created from input
	 */
	public static String generateFileName(String name) {
		return convertToFSName(name) + "/part";
	}


	/**
	 * Converts an RDF literal with datatype into corresponding Pig Latin representation, if possible.
	 * Unsupported types remain as they are.
	 * 
	 * @param literal
	 * @return literal parsed to Pig Latin representation
	 */
	public static String toPigTypes(String literal) {
		// if it is not a literal, simply return it
		if (!literal.startsWith("\"")) {
			return literal;
		}
		// extract the literal out of the (typed) literal string
		int endOfLiteral = literal.lastIndexOf("\"");
		//if (literal.endsWith("xsd:string") || literal.endsWith("<http://www.w3.org/2001/XMLSchema#string>"))
		//	return literal.substring(0, endOfLiteral+1);
		if (literal.endsWith("xsd:integer") || literal.endsWith("<http://www.w3.org/2001/XMLSchema#integer>")
				|| literal.endsWith("xsd:int") || literal.endsWith("<http://www.w3.org/2001/XMLSchema#int>"))
			return literal.substring(1, endOfLiteral);
		else if (literal.endsWith("xsd:long") || literal.endsWith("<http://www.w3.org/2001/XMLSchema#long>"))
			return literal.substring(1, endOfLiteral) + "L";
		else if (literal.endsWith("xsd:float") || literal.endsWith("<http://www.w3.org/2001/XMLSchema#float>"))
			return literal.substring(1, endOfLiteral) + "F";
		else if (literal.endsWith("xsd:double") || literal.endsWith("<http://www.w3.org/2001/XMLSchema#double>")
				|| literal.endsWith("xsd:decimal") || literal.endsWith("<http://www.w3.org/2001/XMLSchema#decimal>"))
			return literal.substring(1, endOfLiteral);
		else if (literal.endsWith("xsd:dateTime") || literal.endsWith("<http://www.w3.org/2001/XMLSchema#dateTime>"))
			return literal.substring(1, endOfLiteral);
		else
			return literal;
	}
}
