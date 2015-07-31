package pigsparql.pig.op;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.util.FmtUtils;

import pigsparql.mapreduce.Util;
import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;
import pigsparql.rdf.PrefixMode;


public class PigBGPVP extends PigBGPBase {

	public PigBGPVP(OpBGP _opBGP, PrefixMapping _prefixes) {
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


	@Override
	protected String loadFilterProject() {
		String triplePatterns = "";
		/*
		 * Iterate over all Triples in the BGP:
		 * 1. Produce a LOAD statement for each distinct predicate
		 * 2. Produce a FILTER for the Triple if there is at least one bound component (s,o)
		 * 3. Project each Triple on its variables
		 */
		ArrayList<Node> predicates = new ArrayList<Node>();
		List<Triple> triples = opBGP.getPattern().getList();
		Iterator<Triple> triplesIterator = triples.listIterator();

		// Start enumerating at 0
		int i = 0;
		while (triplesIterator.hasNext()) {
			Triple triple = triplesIterator.next();
			Node subject = triple.getSubject();
			Node predicate = triple.getPredicate();
			Node object = triple.getObject();
			String relName = (predicate.isVariable() ? "inputData" : Util.convertToFSName(FmtUtils.stringForNode(predicate, prefixes)));

			/*
			 * LOAD
			 */
			String load = "";
			if (!predicates.contains(predicate)) {
				predicates.add(predicate);
				load += relName + " = LOAD '$" +Tags.indata + "/" + relName + "' USING " + Tags.vpLoader + " AS ";
				load += (predicate.isVariable() ? "(s,p,o)" : "(s,o)") + " ; \n";
			}
			triplePatterns += load;

			/*
			 * FILTER
			 */
			String filter = "f" + i + " = FILTER " + relName + " BY ";
			boolean noFilter = true;
			if (subject.isURI() || subject.isBlank()) {
				// subject is bound -> add to Filter
				filter += "s == '" + FmtUtils.stringForNode(subject, prefixes) + "'";
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
			// Projection on the filtered Triple or Tags.indata if there is no Filter for this Triple (nothing bound)
			projection += " = FOREACH " + (noFilter ? relName : "f" + i) + " GENERATE ";
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

			triplePatterns += "\n";
			i++;
		}
		return triplePatterns;
	}


	@Override
	public void visit(PigOpVisitor pigOpVisitor) {
		pigOpVisitor.visit(this);
	}

}
