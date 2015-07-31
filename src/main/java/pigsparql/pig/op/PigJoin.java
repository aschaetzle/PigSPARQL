package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;

import java.util.ArrayList;

import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;

/**
 *
 * @author Alexander Schaetzle
 */
public class PigJoin extends PigOp2 {

    @SuppressWarnings("unused")
	private final OpJoin opJoin;


    public PigJoin(OpJoin _opJoin, PigOp _leftOp, PigOp _rightOp, PrefixMapping _prefixes) {
        super(_leftOp, _rightOp, _prefixes);
        opJoin = _opJoin;
        resultName = Tags.JOIN;
    }


    @Override
    public String getName() {
        return Tags.PIG_JOIN;
    }


    @Override
    public String translate(String _resultName) {
        resultName = _resultName;
        varsWithNulls.addAll(leftOp.getVarsWithNulls());
        varsWithNulls.addAll(rightOp.getVarsWithNulls());
        String join = "-- " + Tags.JOIN + "\n";

        join += generateJoin();

        return join;
    }

    
    private String generateJoin() {
        String join = "";
        boolean needForeach = true;
        ArrayList<String> sharedVars = getSharedVars();
        if (checkForNullJoin(sharedVars)) {
            throw new UnsupportedOperationException("Query leads to a Join on Null values!");
        }

        if (sharedVars.isEmpty()) {
            join += resultName + " = CROSS " + leftOp.getResultName() + ", " + rightOp.getResultName();
            resultSchema.addAll(leftOp.getSchema());
            resultSchema.addAll(rightOp.getSchema());
            needForeach = false;
        }
        else {
            join += "j1 = JOIN ";
            String joinArg = toArgumentList(sharedVars);
            join += leftOp.getResultName() + " BY " + joinArg + ", ";
            join += rightOp.getResultName() + " BY " + joinArg;
        }
        join += " PARALLEL $reducerNum ;\n";

        if(needForeach) {
            join += generateJoinProjection("j1",resultName);
        }
        
        return join;
    }


    /* OLD FOREACH
     * 
    private String generateForeach() {
        String foreach = resultName + " = FOREACH j1 GENERATE ";
        ArrayList<String> leftOpSchema = leftOp.getSchema();
        ArrayList<String> rightOpSchema = rightOp.getSchema();

        String field = leftOpSchema.get(0);
        // schema of left subOp completely
        foreach += "$0" + " AS " + field;
        resultSchema.add(field);
        for (int i=1; i<leftOpSchema.size(); i++) {
            field = leftOpSchema.get(i);
            foreach += ", $" + i + " AS " + field;
            resultSchema.add(field);
        }

        // schema of right subOp without shared variables
        for (int i=0; i<rightOpSchema.size(); i++) {
            field = rightOpSchema.get(i);
            if (!leftOpSchema.contains(field)) {
                foreach += ", $" + (leftOpSchema.size()+i) + " AS " + field;
                resultSchema.add(field);
            }
        }
        foreach += " ;\n";
        return foreach;
    }
     */


    @Override
    public void visit(PigOpVisitor pigOpVisitor) {
        pigOpVisitor.visit(this);
    }

}
