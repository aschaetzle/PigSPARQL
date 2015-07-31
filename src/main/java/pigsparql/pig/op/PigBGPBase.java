package pigsparql.pig.op;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.util.FmtUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;
import pigsparql.rdf.PrefixMode;


/**
 * 
 * @author Alexander Schaetzle
 */
public class PigBGPBase extends PigBGP {

	protected String joinBlock;


	public PigBGPBase(OpBGP _opBGP, PrefixMapping _prefixes) {
		super(_opBGP, _prefixes);
		resultName = Tags.BGP;
	}


	@Override
	public String translate(String _resultName) {
		resultName = _resultName;
		String bgp = "-- " + Tags.BGP + "\n";

		// empty PrefixMapping when prefixes should be expanded
		if (Tags.pMode == PrefixMode.EXPAND) {
			prefixes = PrefixMapping.Factory.create();
		}

		bgp += loadFilterProject();
		bgp += joinTriplePatterns();

		return bgp;
	}


	/**
	 * Generate FILTER statements for Triple Patterns.
	 * 
	 * @return
	 */
	protected String loadFilterProject() {
		String triplePatterns = "";
		/*
		 * Now iterate over all Triples in the BGP:
		 * 1. Produce a FILTER for the Triple if there is at least one bound component (s,p,o)
		 * 2. Project each Triple on its variables
		 */
		List<Triple> triples = opBGP.getPattern().getList();
		Iterator<Triple> triplesIterator = triples.listIterator();
		
		// Start enumerating at 0
		int i = 0;
		while (triplesIterator.hasNext()) {
			Triple triple = triplesIterator.next();

			/*
			 * FILTER
			 */
			String filter = "f" + i + " = FILTER " + Tags.indata + " BY ";
			boolean noFilter = true;
			Node subject = triple.getSubject();
			Node predicate = triple.getPredicate();
			Node object = triple.getObject();
			if (subject.isURI() || subject.isBlank()) {
				// subject is bound -> add to Filter
				filter += "s == '" + FmtUtils.stringForNode(subject, prefixes) + "'";
				noFilter = false;
			}
			if (predicate.isURI()) {
				// predicate is bound -> add to Filter
				filter += (noFilter ? "" : " AND ") + "p == '" + FmtUtils.stringForNode(predicate, prefixes) + "'";
				noFilter = false;
			}
			if (object.isURI() || object.isLiteral() || object.isBlank()) {
				// object is bound -> add to Filter
				filter += (noFilter ? "" : " AND ") + "o == '" + FmtUtils.stringForNode(object, prefixes) + "'";
				noFilter = false;
			}
			filter += " ;";
			// output Filter only if there is at least one argument to the Filter
			if (!noFilter) {
				triplePatterns += filter + "\n";
			}

			/*
			 * PROJECTION on the variables
			 */
			// there will be no joins if the BGP is of size 1 -> Triple gets named with the resultName
			String projection = (triples.size() == 1) ? resultName : "t" + i;
			// Projection on the filtered Triple or indata if there is no Filter for this Triple (nothing bound)
			projection += " = FOREACH " + (noFilter ? Tags.indata : "f" + i) + " GENERATE ";
			ArrayList<String> tripleSchema = getSchemaOfTriple(triple);
			boolean noProject = true;
			if (!tripleSchema.get(0).equals(Tags.NO_VAR)) {
				// subject is a variable
				projection += "s AS " + tripleSchema.get(0);
				noProject = false;
			}
			if (!tripleSchema.get(1).equals(Tags.NO_VAR)) {
				// predicate is a variable
				projection += (noProject ? "" : ", ") + "p AS " + tripleSchema.get(1);
				noProject = false;
			}
			if (!tripleSchema.get(2).equals(Tags.NO_VAR)) {
				// object is a variable
				projection += (noProject ? "" : ", ") + "o AS " + tripleSchema.get(2);
				noProject = false;
			}
			projection += " ;";
			// output Projection only if there is at least one variable in the Triple
			if (!noProject) {
				triplePatterns += projection + "\n";
			}

			i++;
		}

		return triplePatterns;
	}


	/**
	 * Generate JOIN statements for Triple Patterns.
	 * 
	 * @return
	 */
	protected String joinTriplePatterns() {
		List<Triple> triples = opBGP.getPattern().getList();
		joinBlock = "";

		Triple triple = triples.get(0);
		ArrayList<String> tripleVars = getVarsOfTriple(triple);
		// Schema of the first Triple is completely added to the resultSchema -> no duplicates possible
		addToResultSchema(tripleVars);

		int nextTriple = 1;
		ArrayList<String> joinVars;

		/*
		 * Iterate over all Triples in the BGP:
		 * 1. If a Triple has no shared variables with the current resultSchema we have to do a CROSS
		 * 2. If a Triple ha shared variables with the current resultSchema we can do a JOIN
		 * Successive CROSS statements can be reduced to one big CROSS
		 * Successive JOIN statements on the same variables can be reduced to one big JOIN
		 */
		while (nextTriple < triples.size()) {
			triple = triples.get(nextTriple);
			tripleVars = getVarsOfTriple(triple);
			joinVars = getSharedVars(resultSchema, tripleVars);
			int endPos;
			if (joinVars.isEmpty()) {
				/*
				 * Triple has no shared variables with the current resultSchema -> CROSS
				 * nextCross will generate one CROSS of all successive Triples with no shared variables
				 * nextCross returns the index of the last Triple in the generated CROSS
				 */
				endPos = nextCross(nextTriple);
			}
			else {
				/*
				 * Triple has shared variables with the current resultSchema -> JOIN
				 * nextJoin will generate one JOIN of all successive Triples with the same Join variables
				 * nextJoin returns the index of the last Triple in the generated JOIN
				 */
				endPos = nextJoin(nextTriple, joinVars);
			}
			// proceed with next Triple
			nextTriple = endPos + 1;
		}

		return joinBlock;
	}


	/**
	 * 
	 * @param start
	 * @param joinVars
	 * @return
	 */
	protected int nextJoin(int start, ArrayList<String> joinVars) {
		// Triples are joined on the joinVars
		String joinArg = toArgumentList(joinVars);
		// Projection arguments initially contain everything from the current resultSchema
		String projectArgs = addToProjectArgs("", null, (start == 1 ? "t0" : resultName), resultSchema);

		/*
		 * JOIN
		 */
		List<Triple> triples = opBGP.getPattern().getList();
		Triple triple = triples.get(start);
		ArrayList<String> tripleVars = getVarsOfTriple(triple);
		
		// start with joining current result relation (t0 or previous join) with t(start)
		String joinArguments = (start == 1 ? "t0" : resultName) + " BY " + joinArg + ", t" + start + " BY " + joinArg;
		// add variables of t(start) that do not already exist in the current resultSchema to the projection arguments
		projectArgs = addToProjectArgs(projectArgs, resultSchema, "t" + start, tripleVars);
		// update resultSchema -> add additional variables that do not already exist in the current resultSchema
		addToResultSchema(tripleVars);

		/*
		 * Successive Triples which also result in a JOIN on the same variables can be added to this JOIN
		 */
		ArrayList<String> sharedVars;
		int endIndex = start + 1;
		while (endIndex < triples.size()) {
			triple = triples.get(endIndex);
			tripleVars = getVarsOfTriple(triple);
			sharedVars = getSharedVars(resultSchema, tripleVars);
			if (joinVars.containsAll(sharedVars) && sharedVars.containsAll(joinVars)) {
				// joinVars and sharedVars contain the same elements (perhaps not in the same order) -> add Triple to this JOIN
				joinArguments += ", t" + endIndex + " BY " + joinArg;
				// add variables of the current triple that do not already exist in the current resultSchema to the projection arguments
				projectArgs = addToProjectArgs(projectArgs, resultSchema, "t" + endIndex, tripleVars);
				// update resultSchema -> add additional variables that do not already exist in the current resultSchema
				addToResultSchema(tripleVars);
				endIndex++;
			}
			// Triple can't be added to this JOIN -> END OF JOIN
			else
				break;
		}

		String join = resultName + " = JOIN " + joinArguments + " ;";
		joinBlock += join + "\n";

		/*
		 * PROJECTION -> drop duplicated entries generated by the JOIN statement
		 */
		String foreach = resultName + " = FOREACH " + resultName + " GENERATE " + projectArgs + " ;";
		joinBlock += foreach + "\n";

		return endIndex - 1;
	}
	
	
	/**
	 * Cross Product between Triple Patterns.
	 * Hopefully we never have to use this.
	 * 
	 * @param start
	 * @return
	 */
	protected int nextCross(int start) {
		List<Triple> triples = opBGP.getPattern().getList();
		Triple triple = triples.get(start);
		ArrayList<String> tripleVars = getVarsOfTriple(triple);

		String crossArgs = (start == 1 ? "t0" : resultName) + ", t" + start;
		// update resultSchema -> add variables of t(start)
		addToResultSchema(tripleVars);

		/*
		 * Successive Triples which also result in a CROSS can be added to this CROSS
		 */
		ArrayList<String> sharedVars;
		int endIndex = start + 1;
		while (endIndex < triples.size()) {
			triple = triples.get(endIndex);
			tripleVars = getVarsOfTriple(triple);
			sharedVars = getSharedVars(resultSchema, tripleVars);
			if (sharedVars.isEmpty()) {
				// add Triple to this CROSS
				crossArgs += ", t" + endIndex;
				// update resultSchema -> add variables of t(endIndex)
				addToResultSchema(tripleVars);
				endIndex++;
			}
			// Triple has shared variables -> END OF CROSS
			else
				break;
		}

		String cross = resultName + " = CROSS " + crossArgs + " ;";
		joinBlock += cross + "\n";

		return endIndex - 1;
	}


	@Override
	public void visit(PigOpVisitor pigOpVisitor) {
		pigOpVisitor.visit(this);
	}

}