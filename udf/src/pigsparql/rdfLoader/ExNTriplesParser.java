package pigsparql.rdfLoader;

/**
 * Parser for RDF Data encoded as extended N-Triple.
 * The parser takes a String with a RDF Triple in N-Triple syntax as input
 * and returns the subject, predicate and object of the Triple in an array.
 * Beyond the syntax of N-Triples the parser also supports some commonly used
 * prefixes as well as the prefixes used in the SP2Bench SPARQL Performance Benchmark.
 * The parser also supports Blank Nodes and typed Literals.
 * The user can decide to expand the prefixes in the returned Triple or not.
 *
 * Supported prefixes: foaf, bench, xsd, dc, dcterms, dctype, rdf, rdfs, swrc, rss, owl, person
 *
 * @see <a href="http://dbis.informatik.uni-freiburg.de/index.php?project=SP2B">SP2Bench SPARQL Performance Benchmark</a>
 *
 * @author Alexander Schaetzle
 * @version 1.1
 */
public class ExNTriplesParser {

    /**
     * The predefined prefixes that are supported by the parser.
     */
    private static String[][] prefixes = new String[][]{
        {"foaf:","<http://xmlns.com/foaf/0.1/"},
        {"bench:","<http://localhost/vocabulary/bench/"},
        {"xsd:","<http://www.w3.org/2001/XMLSchema#"},
        {"dc:","<http://purl.org/dc/elements/1.1/"},
        {"dcterms:","<http://purl.org/dc/terms/"},
        {"dctype:","<http://purl.org/dc/dcmitype/"},
        {"rdf:","<http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
        {"rdfs:","<http://www.w3.org/2000/01/rdf-schema#"},
        {"swrc:","<http://swrc.ontoware.org/ontology#"},
        {"rss:","<http://purl.org/rss/1.0/"},
        {"owl:","<http://www.w3.org/2002/07/owl#"},
        {"person:","<http://localhost/persons/"},
        {"ex:","<http://example.org/"}
    };

    /**
     * Should the parser expand the prefixes in the Triple or not.
     */
    private static boolean expand = false;

    private static String fieldDel = " ";


    /**
     * Parses a RDF Triple in extended N-Triple syntax given as String.
     * Subject, Predicate and Object have to be separated with the given delimiter.
     * Subjects can be URIs or Blank Nodes, Predicates must be URIs and
     * Objects can be URIs, Blank Nodes or Typed Literales.
     * Returns a String array with 3 fields [subject, predicate, object]
     * or null if the line could not be parsed.
     * The user can decide whether the prefixes should be expanded or not.
     *
     * @see <a href="http://www.w3.org/2001/sw/RDFCore/ntriples/">W3C N-Triples</a>
     * @see <a href="http://www.w3.org/TR/rdf-concepts/#section-Literals">RDF Concepts: Literals</a>
     * @see <a href="http://www.w3.org/TR/rdf-concepts/#section-Graph-Literal">RDF Abstract Syntax: Literals</a>
     *
     * @param line  Input Line to parse
     * @param _expand   expand prefixes or not
     * @return  String array [subject, predicate, object] or null
     */
    public static String[] parseTriple(String line, String delimiter, boolean _expand) {
        expand = _expand;
        if (delimiter != null) fieldDel = delimiter;
        // first, discard leading and trailing whitespaces
        //line = line.substring(0, line.lastIndexOf(".")+1);
        line = line.trim();

        // line has to end with a dot, otherwise it is a syntax error
        if(line.endsWith(".")) {
            // remove trailing dot and discard possible whitespaces
            line = line.substring(0, line.length()-1).trim();
        }
        else return null;

        // split input line into 3 fields using the given delimiter
        String[] fields = splitTriple(line);
        if(fields == null) return null;

        /*
         * Parse subject, predicate and object.
         * If any of these can't be parsed a null value is returned.
         * When parsing fails there is a syntax error in the corresponding field.
         */
        String subject = parseSubject(fields[0]);
        if(subject == null) return null;
        String predicate = parsePredicate(fields[1]);
        if(predicate == null) return null;
        String object = parseObject(fields[2]);
        if(object == null) return null;

        // return the parsed Triple
        return new String[]{subject, predicate, object};
    }

    /**
     * Splits the input into 3 fields (Subject, Predicate, Object).
     *
     * @param input input to be splitted
     * @return  Field-Array or null if input can't be splitted
     */
    private static String[] splitTriple(String input) {
        // a Triple has 3 fields
        String[] fields = new String[3];

        // extract first field
        int delPos = input.indexOf(fieldDel);
        if (delPos == -1) return null;
        fields[0] = input.substring(0, delPos); // Subject
        if (fields[0] == null) return null;
        input = input.substring(delPos+1);
        // delete leading delimiters (fields can be delimited by more than one delimiter)
        while (input.startsWith(fieldDel)) {
            input = input.substring(1);
        }

        // extract second field
        delPos = input.indexOf(fieldDel);
        if (delPos == -1) return null;
        fields[1] = input.substring(0, delPos); // Predicate
        if (fields[1] == null) return null;
        input = input.substring(delPos+1);
        // delete leading delimiters (fields can be delimited by more than one delimiter)
        while (input.startsWith(fieldDel)) {
            input = input.substring(1);
        }

        // rest of input is the last field
        fields[2] = input; // Object

        return fields;
    }

    /**
     * Parses the Subject of the Triple.
     * Subjects can be URIs or Blank Nodes.
     *
     * @param input Subject to be parsed
     * @return  parsed Subject or null (syntax error)
     */
    private static String parseSubject(String input) {
        if(isURI(input) || isBlankNode(input)) {
            return expandPrefix(input);
        }
        else return null;
    }

    /**
     * Parses the Predicate of the Triple.
     * Predicates must be URIs.
     *
     * @param input Predicate to be parsed
     * @return  parsed Predicate or null (syntax error)
     */
    private static String parsePredicate(String input) {
        if(isURI(input)) {
            return expandPrefix(input);
        }
        else return null;
    }

    /**
     * Parses the Object of the Triple.
     * Objects can be URIs, Blank Nodes or Literals (plain or typed).
     *
     * @param input Object to be parsed
     * @return  parsed Object or null (syntax error)
     */
    private static String parseObject(String input) {
        if(isURI(input) || isBlankNode(input)) {
            return expandPrefix(input);
        }
        // if Object is not an URI or Blank Node it must be a Literal
        else return parseLiteralObject(input);
    }

    /**
     * Parses a Literal Object.
     * Literal Objects can be plain or typed.
     *
     * @param input Literal Object to be parsed
     * @return  parsed Literal or null (syntax error)
     */
    private static String parseLiteralObject(String input) {
        // check whether it is a number or a plain literal
        if(input.matches("[0-9]+") || input.matches("[0-9]+\\.[0-9]+") || input.matches("\"(.)*\"")) {
            return input;
        }
        // if it is not a number nor a plain literal it has to be a typed literal
        else return parseTypedLiteralObject(input);
    }

    /**
     * Parses a Typed Literal Object.
     * Typed Literals can have a Language Tag (@) or a Datatype (^^)
     *
     * @param input Typed Literal to be parsed
     * @return  parsed Typed Literal or null (syntax error)
     */
    private static String parseTypedLiteralObject(String input) {
        // typed literals have to start with quotation mark (")
        if(!input.startsWith("\"")) {
            return null; // syntax error
        }
        // extract the literal out of the typed literal
        int endOfLiteral = input.lastIndexOf("\"")+1;
        String literal = input.substring(0, endOfLiteral);
        // empty literals ("") are not allowed
        if(literal.length() <= 2) {
            return null; // syntax error
        }
        // check if it has a language tag
        else if(input.substring(endOfLiteral, endOfLiteral+1).equals("@")) {
            /* extract the language tag out of the typed literal,
             * normalize the language tag and return the normalized typed literal
             */
            String languageTag = input.substring(endOfLiteral+1, input.length());
            languageTag = languageTag.toLowerCase();
            return literal+"@"+languageTag;
        }
        // check if it has a datatype
        else if(input.substring(endOfLiteral, endOfLiteral+2).equals("^^")) {
            /* extract the datatype out of the typed literal,
             * expand prefixes if wanted and return the typed literal
             */
            String datatype = input.substring(endOfLiteral+2, input.length());
            return literal+"^^"+expandPrefix(datatype);
        }
        // if the typed literal has neither a language tag nor a datatype it is a syntax error
        else return null;
    }


    /**
     * Checks if the the input is an URI.
     * URIs are encapsulated in brackets (<>) or have a leading prefix.
     *
     * @param input
     * @return  true, if input is a URI
     */
    private static boolean isURI(String input){
        if(input.startsWith("<") && input.endsWith(">")) {
            return true;
        }
        else if(startsWithPrefix(input))
            return true;
        else return false;
    }

    /**
     * Checks if the the input is a Blank Node.
     * Blank Nodes have a leading _: .
     *
     * @param input
     * @return  true, if input is a Blank Node
     */
    private static boolean isBlankNode(String input){
        if(input.startsWith("_:")) {
            return true;
        }
        else return false;
    }

    /**
     * Checks if the input starts with a predefined prefix.
     * @param input
     * @return  true, is input starts with a predefined prefix
     */
    private static boolean startsWithPrefix(String input) {
        for(int i=0; i<prefixes.length; i++) {
            if(input.startsWith(prefixes[i][0]))
                return true;
        }
        return false;
    }

    /**
     * Expands predefined leading Prefixes if expand is set true.
     * If expand is not set true it just returns the input.
     *
     * @param input
     * @return  input with expanded Prefix if expand is set true
     */
    private static String expandPrefix(String input) {
        if(!expand) return input;
        // URIs without a prefix or Blank Nodes don't have to be treated
        if(input.startsWith("<") && input.endsWith(">")) {
            return input;
        }
        if(input.startsWith("_:")) {
            return input;
        }
        // check if one of the predefined prefixes matches
        for(int i=0; i<prefixes.length; i++){
            if(input.startsWith(prefixes[i][0])) {
                // replace prefix if a match is found
                input = input.replace(prefixes[i][0], prefixes[i][1]);
                return input+">"; // close URI
            }
        }
        return input;
    }

}
