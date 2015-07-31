package pigsparql.pig.op;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;

import java.util.ArrayList;

import pigsparql.pig.Tags;


/**
 * 
 * @author Alexander Schaetzle
 */
public abstract class PigBGP extends PigOp0 {

	protected final OpBGP opBGP;


	public PigBGP(OpBGP _opBGP, PrefixMapping _prefixes) {
		super(_prefixes);
		opBGP = _opBGP;
	}


	@Override
	public String getName() {
		return Tags.PIG_BGP;
	}


	protected ArrayList<String> getSchemaOfTriple(Triple t) {
		ArrayList<String> schema = new ArrayList<String>();
		Node subject = t.getSubject();
		Node predicate = t.getPredicate();
		Node object = t.getObject();
		if (subject.isVariable())
			schema.add(subject.getName());
		else
			schema.add(Tags.NO_VAR);
		if (predicate.isVariable())
			schema.add(predicate.getName());
		else
			schema.add(Tags.NO_VAR);
		if (object.isVariable())
			schema.add(object.getName());
		else
			schema.add(Tags.NO_VAR);
		return schema;
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

}