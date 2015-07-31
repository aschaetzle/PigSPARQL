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
import java.io.PrintWriter;

import org.apache.log4j.Logger;

import pigsparql.pig.Tags.Optimizer;
import pigsparql.pig.op.PigOp;
import pigsparql.rdf.PrefixMode;
import pigsparql.sparql.AlgebraTransformer;
import pigsparql.sparql.opt.BGPOptimizerNoStats;
import pigsparql.sparql.opt.TransformFilterVarEquality;


/**
 * Main Class of the PigSPARQL Compiler.
 * This class generates the Algebra Tree for a SPARQL query,
 * performs some optimizations, translates the Algebra tree into an
 * PigOp tree, initializes the translation and collects the final output.
 * 
 * @author Alexander Schaetzle
 */
public class PigCompiler {

	String inputFile;
	String outputFile;
	private String pigScript;
	private PigOp pigOpRoot;

	// Define a static logger variable so that it references the corresponding Logger instance
	private static final Logger logger = Logger.getLogger(PigCompiler.class);


	/**
	 * Constructor of the PigSPARQL translator.
	 * 
	 * @param _inputFile
	 *            SPARQL query to be translated
	 * @param _outputFile
	 *            Output script of the translator
	 */
	public PigCompiler(String _inputFile, String _outputFile) {
		pigScript = "";
		inputFile = _inputFile;
		outputFile = _outputFile;
	}


	/**
	 * Sets the delimiter of the RDF Triples.
	 * 
	 * @param _delimiter
	 */
	public void setDelimiter(String delimiter) {
		Tags.delimiter = delimiter;
	}


	/**
	 * Sets if Vertical Partitioning is used for Query compilation.
	 * 
	 * @param value
	 */
	public void setPartitioning(boolean value) {
		Tags.verticalPartitioning = value;
	}


	/**
	 * Sets if prefixes in the RDF Triples should be expanded or not.
	 * 
	 * @param value
	 */
	public void setPrefixMode(PrefixMode pMode) {
		Tags.pMode = pMode;
	}


	/**
	 * Enables or disables optimizations of the SPARQL Algebra.
	 * Optimizer is enabled by default.
	 * 
	 * @param value
	 */
	public void setOptimizer(Optimizer opt) {
		Tags.optimizer = opt;
	}


	/**
	 * Translates the SPARQL query into a Pig Latin program.
	 */
	public void translateQuery() {
		// Parse input query
		Query query = QueryFactory.read("file:" + inputFile);
		// Get prefixes defined in the query
		PrefixMapping prefixes = query.getPrefixMapping();

		// Generate translation logfile
		PrintWriter logWriter;
		try {
			logWriter = new PrintWriter(outputFile + ".log");
		} catch (FileNotFoundException ex) {
			logger.warn("Cannot open translation logfile, using stdout instead!", ex);
			logWriter = new PrintWriter(System.out);
		}

		// Output original query to log
		logWriter.println("SPARQL Input Query:");
		logWriter.println("###################");
		logWriter.println(query);
		logWriter.println();
		// Print Algebra Using SSE, true -> optimiert
		// PrintUtils.printOp(query, true);

		// Generate Algebra Tree of the SPARQL query
		Op opRoot = Algebra.compile(query);

		// Output original Algebra Tree to log
		logWriter.println("Algebra Tree of Query:");
		logWriter.println("######################");
		logWriter.println(opRoot.toString(prefixes));
		logWriter.println();

		// Optimize Algebra Tree if optimizer is enabled
		if (Tags.optimizer != Optimizer.NONE) {
			opRoot = optimizeAlgebra(opRoot);
			// Output optimized Algebra Tree to log file
			logWriter.println("Optimized Algebra Tree of Query:");
			logWriter.println("Optimization level: " + Tags.optimizer.toString());
			logWriter.println("################################");
			logWriter.println(opRoot.toString(prefixes));
			logWriter.println();
		}

		// Transform SPARQL Algebra Tree in PigOp Tree
		pigOpRoot = new AlgebraTransformer(prefixes).transform(opRoot);

		// Print PigOp Tree to log
		logWriter.println("PigOp Tree:");
		logWriter.println("###########");
		PigOpPrettyPrinter.print(logWriter, pigOpRoot);
		// close log file
		logWriter.close();

		// Walk through PigOp Tree and generate translation
		// Initial statements in Pig script
		generateInitAndLoad();
		// Then translate the query
		pigScript += new PigOpCompiler().translate(pigOpRoot);
		// Finally generate store command
		generateStore();

		// Print resulting Pig Latin program to output file
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


	private Op optimizeAlgebra(Op opRoot) {
		/*
		 * Algebra Optimierer fuehrt High-Level Transformationen aus (z.B. Filter Equalilty)
		 * -> nicht BGP reordering
		 * 
		 * zunaechst muss gesetzt werden was alles optimiert werden soll, z.B.
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
		 * 1. Transformationen der SPARQL-Algebra bis auf FilterPlacement -> koennte Kreuzprodukte erzeugen
		 * 2. BGPOptimizer -> Neuanordnung der Triple im BGP zur Vermeidung von Kreuzprodukten und zur Minimierung von Joins
		 * 3. FilterPlacement -> Vorziehen des Filters soweit moeglich
		 */

		// Perform Join optimization only if optimization level is set to ALL
		if (Tags.optimizer == Optimizer.ALL) {
			TransformJoinStrategy joinStrategy = new TransformJoinStrategy();
			opRoot = Transformer.transform(joinStrategy, opRoot);
		}

		// Perform filter optimizations if optimization level is set to FILTER or ALL
		if (Tags.optimizer == Optimizer.FILTER || Tags.optimizer == Optimizer.ALL) {
			// ARQ optimization of Filter conjunction
			TransformFilterConjunction filterConjunction = new TransformFilterConjunction();
			opRoot = Transformer.transform(filterConjunction, opRoot);

			// ARQ optimization of Filter disjunction
			TransformFilterDisjunction filterDisjunction = new TransformFilterDisjunction();
			opRoot = Transformer.transform(filterDisjunction, opRoot);

			// ARQ optimization of Filter equality
			TransformFilterEquality filterEquality = new TransformFilterEquality();
			opRoot = Transformer.transform(filterEquality, opRoot);

			// Own optimization of Filter variable equality
			TransformFilterVarEquality filterVarEquality = new TransformFilterVarEquality();
			opRoot = filterVarEquality.transform(opRoot);
		}

		// Perform BGP optimization if optimization level is set to BGP, FILTER or ALL
		if (Tags.optimizer == Optimizer.BGP || Tags.optimizer == Optimizer.FILTER || Tags.optimizer == Optimizer.ALL) {
			// Own BGP optimizer using variable counting heuristics
			BGPOptimizerNoStats bgpOptimizer = new BGPOptimizerNoStats();
			opRoot = bgpOptimizer.optimize(opRoot);
		}

		// Perform filter optimizations if optimization level is set to FILTER or ALL
		// Filter Placement must be done last!
		if (Tags.optimizer == Optimizer.FILTER || Tags.optimizer == Optimizer.ALL) {
			// ARQ optimization of Filter placement
			TransformFilterPlacement filterPlacement = new TransformFilterPlacement();
			opRoot = Transformer.transform(filterPlacement, opRoot);
		}
		return opRoot;
	}


	/**
	 * Generates initial commands of the Pig Latin program.
	 */
	private void generateInitAndLoad() {
		String init = "SET default_parallel $reducerNum ; \n";
		init += "SET job.name '$jobName ON $" + Tags.indata + " STORED TO $" + Tags.outdata + "' ; \n";
		if (!Tags.verticalPartitioning) {
			init += "REGISTER " + Tags.udf + " ;\n\n";
			init += "-- load input data \n";
			init += Tags.indata + " = LOAD '$" + Tags.indata + "' USING " + Tags.rdfLoader;
			if (Tags.pMode == PrefixMode.EXPAND)
				init += "('" + Tags.delimiter + "','expand')";
			else if (Tags.pMode == PrefixMode.COLLAPSE)
				init += "('" + Tags.delimiter + "','collapse')";
			else
				init += "('" + Tags.delimiter + "')";
			init += " AS (s,p,o) ;";
			init += "\n";
		}
		appendToScript(init);
	}


	/**
	 * Generates final store command of the Pig Latin program.
	 */
	private void generateStore() {
		String store = "-- store results into output\n";
		store += "STORE " + pigOpRoot.getResultName() + " INTO '$" + Tags.outdata + "' USING " + Tags.resultWriter + "('\\t') ;";
		// Currently we use PigStorage to store output
		// ArrayList<String> resultSchema = pigOpRoot.getSchema();
		// String schema = "?" + resultSchema.get(0);
		// for(int i=1; i<resultSchema.size(); i++) {
		// schema += ", ?" + resultSchema.get(i);
		// }
		// store += "('" + schema + "') ;";
		appendToScript(store);
	}


	/**
	 * Appends the argument to the end of the Pig Latin program.
	 * 
	 * @param block
	 */
	private void appendToScript(String block) {
		// Only append if not empty
		if (!block.equals("")) {
			pigScript += block + "\n";
		}
	}

}