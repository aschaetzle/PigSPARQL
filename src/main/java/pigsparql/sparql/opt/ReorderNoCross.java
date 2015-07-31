package pigsparql.sparql.opt;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;

import java.util.ArrayList;
import java.util.List;


/**
 * 
 * @author Alexander Schaetzle
 */
public class ReorderNoCross {

	private ArrayList<String> allJoinVars;
	private ArrayList<String> lastJoinVars;
	private BasicPattern inputPattern;
	private BasicPattern outputPattern;


	public ReorderNoCross() {
		allJoinVars = new ArrayList<String>();
		lastJoinVars = new ArrayList<String>();
		outputPattern = new BasicPattern();
	}


	public BasicPattern reorder(BasicPattern pattern) {
		inputPattern = pattern;
		List<Triple> triples = inputPattern.getList();

		int idx = chooseFirst();
		Triple triple = triples.get(idx);
		outputPattern.add(triple);
		addToJoinVars(getVarsOfTriple(triple));
		triples.remove(idx);

		while (!triples.isEmpty()) {
			idx = chooseNext();
			triple = triples.get(idx);
			outputPattern.add(triple);
			addToJoinVars(getVarsOfTriple(triple));		
			triples.remove(idx);
		}

		return outputPattern;
	}
	
	
	protected int chooseFirst() {
		for (int i = 0; i < inputPattern.size(); i++) {
			if (hasSharedVars(i)) {
				return i;
			}
		}
		return 0;
	}


	protected int chooseNext() {
		ArrayList<String> tripleVars;
		ArrayList<String> sharedVars;
		// first try to choose a triple pattern with the same join variables -> Multijoin
		for (int i = 0; i < inputPattern.size(); i++) {
			tripleVars = getVarsOfTriple(inputPattern.get(i));
			sharedVars = getSharedVars(allJoinVars, tripleVars);
			if (lastJoinVars.size() > 0 && lastJoinVars.size() == sharedVars.size() && lastJoinVars.containsAll(sharedVars)) {
				// lastJoinVars remain unchanged
				return i;
			}
		}
		// otherwise choose next triple pattern with most shared variables (even if it does not have the hightest selectivity)
		// initially start with the first triple pattern as it has the highest selectivity
		int nextTriple = 0;
		int numOfJoinVars = 0;
		lastJoinVars.clear();
		for (int i = 0; i < inputPattern.size(); i++) {
			tripleVars = getVarsOfTriple(inputPattern.get(i));
			sharedVars = getSharedVars(allJoinVars, tripleVars);
			if (sharedVars.size() > numOfJoinVars) {
				lastJoinVars.clear();
				lastJoinVars.addAll(sharedVars);
				nextTriple = i;
				numOfJoinVars = sharedVars.size();
			}
		}
		// if no triple pattern has shared variables, we return the first pattern -> we cannot avoid cross product
		return nextTriple;
	}


	protected boolean hasSharedVars(int triplePos) {
		Triple triple = inputPattern.get(triplePos);
		ArrayList<String> tripleVars = getVarsOfTriple(triple);
		for (int i = 0; i < inputPattern.size(); i++) {
			if (i != triplePos && getSharedVars(getVarsOfTriple(inputPattern.get(i)), tripleVars).size() > 0) {
				return true;
			}
		}
		return false;
	}


	protected ArrayList<String> getVarsOfTriple(Triple t) {
		ArrayList<String> vars = new ArrayList<String>();
		Node subject = t.getSubject();
		Node predicate = t.getPredicate();
		Node object = t.getObject();
		if (subject.isVariable())
			vars.add(subject.getName());
		if (predicate.isVariable())
			vars.add(predicate.getName());
		if (object.isVariable())
			vars.add(object.getName());
		return vars;
	}


	protected ArrayList<String> getSharedVars(ArrayList<String> leftSchema, ArrayList<String> rightSchema) {
		ArrayList<String> sharedVars = new ArrayList<String>();
		for (int i = 0; i < rightSchema.size(); i++) {
			if (leftSchema.contains(rightSchema.get(i)))
				sharedVars.add(rightSchema.get(i));
		}
		return sharedVars;
	}
	
	
	protected void addToJoinVars(ArrayList<String> tripleVars) {
		String field;
		for (int i = 0; i < tripleVars.size(); i++) {
			field = tripleVars.get(i);
			if (!allJoinVars.contains(field)) {
				allJoinVars.add(field);
			}
		}
	}

}
