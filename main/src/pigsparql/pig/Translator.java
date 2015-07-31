package pigsparql.pig;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterConjunction;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterDisjunction;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterEquality;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterPlacement;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformJoinStrategy;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import pigsparql.pig.op.PigOp;
import pigsparql.sparql.AlgebraTransformer;
import pigsparql.sparql.BGPOptimizerNoStats;
import pigsparql.sparql.TransformFilterVarEquality;


/**
 * Main Class of the PigSPARQL translator.
 * This class generates the Algebra Tree for a SPARQL query,
 * performs some optimizations, translates the Algebra tree into an
 * PigOp tree, initializes the translation and collects the final output.
 *
 * @author Alexander Schaetzle
 */
public class Translator {

    String inputFile;
    String outputFile;
    private String pigScript;
    private PigOp pigOpRoot;
    
    // Define a static logger variable so that it references the corresponding Logger instance
    private static Logger logger = Logger.getLogger(Translator.class);


    /**
     * Constructor of the PigSPARQL translator.
     *
     * @param _inputFile SPARQL query to be translated
     * @param _outputFile Output script of the translator
     */
    public Translator(String _inputFile, String _outputFile) {
        pigScript = "";
        inputFile = _inputFile;
        outputFile = _outputFile;
    }

    /**
     * Sets the delimiter of the RDF Triples.
     * @param _delimiter
     */
    public void setDelimiter(String _delimiter) {
        Tags.delimiter = _delimiter;
    }

    /**
     * Sets if pefixes in the RDF Triples should be expanded or not.
     * @param value
     */
    public void setExpandMode(boolean value) {
        Tags.expandPrefixes = value;
    }

    /**
     * Enables or disables optimizations of the SPARQL Algebra.
     * Optimizer is enabled by default.
     * @param value
     */
    public void setOptimizer(boolean value) {
        Tags.optimizer = value;
    }

    /**
     * Enables or disables Join optimizations.
     * Join optimizations are disabled by default
     * @param value
     */
    public void setJoinOptimizer(boolean value) {
        Tags.joinOptimizer = value;
    }

    /**
     * Enables or disables Filter optimizations.
     * Filter optimizations are enabled by default.
     * @param value
     */
    public void setFilterOptimizer(boolean value) {
        Tags.filterOptimizer = value;
    }

    /**
     * Enables or disables BGP optimizations.
     * BGP optimizations are enabled by default.
     * @param value
     */
    public void setBGPOptimizer(boolean value) {
        Tags.bgpOptimizer = value;
    }


    /**
     * Translates the SPARQL query into a Pig Latin program.
     *
     * @throws IOException
     */
    public void translateQuery() {
        //Parse input query
        Query query = QueryFactory.read("file:"+inputFile);
        //Get prefixes defined in the query
        PrefixMapping prefixes = query.getPrefixMapping();

        //Generate translation logfile
        PrintWriter logWriter;
        try {
            logWriter = new PrintWriter(outputFile + ".log");
        } catch (FileNotFoundException ex) {
            logger.warn("Cannot open translation logfile, using stdout instead!", ex);
            logWriter = new PrintWriter(System.out);
        }

        //Output original query to log
        logWriter.println("SPARQL Input Query:");
        logWriter.println("###################");
        logWriter.println(query);
        logWriter.println();
        //Print Algebra Using SSE, true -> optimiert
        //PrintUtils.printOp(query, true);

        //Generate Algebra Tree of the SPARQL query
        Op opRoot = Algebra.compile(query);

        //Output original Algebra Tree to log
        logWriter.println("Algebra Tree of Query:");
        logWriter.println("######################");
        logWriter.println(opRoot.toString(prefixes));
        logWriter.println();

        //Optimize Algebra Tree if optimizer is enabled
        if(Tags.optimizer) {
            /*
             * Algebra Optimierer führt High-Level Transformationen aus (z.B. Filter Equalilty)
             * -> nicht BGP reordering
             *
             * zunächst muss gesetzt werden was alles optimiert werden soll, z.B.
             * ARQ.set(ARQ.optFilterEquality, true);
             * oder
             * ARQ.set(ARQ.optFilterPlacement, true);
             *
             * Danach kann dann Algebra.optimize(op) aufgerufen werden
             */

            /*
             * Algebra.optimize always executes TransformJoinStrategy -> not always wanted
             * ARQ.set(ARQ.optFilterPlacement, false);
             * ARQ.set(ARQ.optFilterEquality, true);
             * ARQ.set(ARQ.optFilterConjunction, true);
             * ARQ.set(ARQ.optFilterDisjunction, true);
             * opRoot = Algebra.optimize(opRoot);
             */

            /*
             * Reihenfolge der Optimierungen wichtig!
             *
             * 1. Transformationen der SPARQL-Algebra bis auf FilterPlacement -> könnte Kreuzprodukte erzeugen
             * 2. BGPOptimizer -> Neuanordnung der Triple im BGP zur Vermeidung von Kreuzprodukten und zur Minimierung von Joins
             * 3. FilterPlacement -> Vorziehen des Filters soweit möglich
             */

            if(Tags.joinOptimizer) {
                TransformJoinStrategy joinStrategy = new TransformJoinStrategy();
                opRoot = Transformer.transform(joinStrategy, opRoot);
            }

            if(Tags.filterOptimizer) {
                //ARQ optimization of Filter conjunction
                TransformFilterConjunction filterConjunction = new TransformFilterConjunction();
                opRoot = Transformer.transform(filterConjunction, opRoot);

                //ARQ optimization of Filter disjunction
                TransformFilterDisjunction filterDisjunction = new TransformFilterDisjunction();
                opRoot = Transformer.transform(filterDisjunction, opRoot);

                //ARQ optimization of Filter equality
                TransformFilterEquality filterEquality = new TransformFilterEquality();
                opRoot = Transformer.transform(filterEquality, opRoot);

                //Own optimization of Filter variable equality
                TransformFilterVarEquality filterVarEquality = new TransformFilterVarEquality();
                opRoot = filterVarEquality.transform(opRoot);
            }

            if(Tags.bgpOptimizer) {
                //Own BGP optimizer using variable counting heuristics
                BGPOptimizerNoStats bgpOptimizer = new BGPOptimizerNoStats();
                opRoot = bgpOptimizer.optimize(opRoot);
            }

            if(Tags.filterOptimizer) {
                //ARQ optimization of Filter placement
                TransformFilterPlacement filterPlacement = new TransformFilterPlacement();
                opRoot = Transformer.transform(filterPlacement, opRoot);
            }

            //Output optimized Algebra Tree to log file
            logWriter.println("optimized Algebra Tree of Query:");
            logWriter.println("################################");
            logWriter.println(opRoot.toString(prefixes));
            logWriter.println();
        }

        //Transform SPARQL Algebra Tree in PigOp Tree
        AlgebraTransformer transformer = new AlgebraTransformer(prefixes);
        pigOpRoot = transformer.transform(opRoot);

        //Print PigOp Tree to log
        logWriter.println("PigOp Tree:");
        logWriter.println("###########");
        PigOpPrettyPrinter.print(logWriter, pigOpRoot);
        //close log file
        logWriter.close();

        //Walk through PigOp Tree and generate translation
        //First load UDFs and load input file
        generateInitAndLoad();
        //Then translate the query
        PigOpTranslator translator = new PigOpTranslator();
        pigScript += translator.translate(pigOpRoot, Tags.expandPrefixes);
        //Finally generate store command
        generateStore();

        //Print resulting Pig Latin program to output file
        PrintWriter pigWriter;
        try {
            pigWriter = new PrintWriter(outputFile);
            pigWriter.print(pigScript);
            pigWriter.close();
        } catch (FileNotFoundException ex) {
            logger.fatal("Cannot open output file!", ex);
            System.exit(-1);
        }
    }


    /**
     * Generates initial commands of the Pig Latin program.
     */
    private void generateInitAndLoad() {
        String init = Tags.defaultReducer + "\n\n";
        init += "REGISTER " + Tags.udf + ";\n\n";
        init += "-- load input data\n";
        init += Tags.indata + " = LOAD '$inputData' USING " + Tags.rdfLoader;
        init += (Tags.expandPrefixes)? "('"+Tags.delimiter+"','expand') ;" : "('"+Tags.delimiter+"','noExpand') AS (s,p,o);";
        init += "\n";
        appendToScript(init);
    }

    /**
     * Generates final store command of the Pig Latin program.
     */
    private void generateStore() {
        String store = "-- store results into output\n";
        store += "STORE " + pigOpRoot.getResultName() + " INTO '$outputData' USING " + Tags.resultWriter + "(' ') ;";
        //Currently we use PigStorage to store output
//        ArrayList<String> resultSchema = pigOpRoot.getSchema();
//        String schema = "?" + resultSchema.get(0);
//        for(int i=1; i<resultSchema.size(); i++) {
//            schema += ", ?" + resultSchema.get(i);
//        }
//        store += "('" + schema + "') ;";
        appendToScript(store);
    }

    /**
     * Appends the argument to the end of the Pig Latin program.
     * @param block
     */
    private void appendToScript(String block) {
        //Only append if not empty
        if (!block.equals("")) {
            pigScript += block + "\n";
        }
    }

}