package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import java.util.ArrayList;
import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;

/**
 *
 * @author Alexander Schaetzle
 */
public class PigUnion extends PigOp2 {

    private OpUnion opUnion;
    private ArrayList<String> leftOuterVars, rightOuterVars, sharedVars;


    public PigUnion(OpUnion _opUnion, PigOp _leftOp, PigOp _rightOp, PrefixMapping _prefixes) {
        super(_leftOp, _rightOp, _prefixes);
        opUnion = _opUnion;
        resultName = Tags.UNION;
    }


    @Override
    public String getName() {
        return Tags.PIG_UNION;
    }


    @Override
    public String translate(String _resultName) {
        resultName = _resultName;
        varsWithNulls.addAll(leftOp.getVarsWithNulls());
        varsWithNulls.addAll(rightOp.getVarsWithNulls());
        String union = "-- " + Tags.UNION + "\n";

        union += generateUnion();

        return union;
    }

    private String generateUnion() {
        String union = "";
        ArrayList<String> leftOpSchema = leftOp.getSchema();
        ArrayList<String> rightOpSchema = rightOp.getSchema();

        // schemas are exactly the same
        if(leftOpSchema.equals(rightOpSchema)) {
            union += resultName + " = UNION ";
            union += leftOp.getResultName() + ", " + rightOp.getResultName();
            resultSchema.addAll(leftOp.getSchema());
        }
        // schemas are the same but in different order
        else if (leftOpSchema.size()==rightOpSchema.size() && leftOpSchema.containsAll(rightOpSchema)) {
            union += "u1 = FOREACH " + rightOp.getResultName() + " GENERATE ";
            union += leftOpSchema.get(0);
            for (int i=1; i<leftOpSchema.size(); i++) {
                union += ", " + leftOpSchema.get(i);
            }
            union += " ;\n";
            union += resultName + " = UNION " + leftOp.getResultName() + ", u1";
            resultSchema.addAll(leftOpSchema);
        }
        // different schemas
        else {
            leftOuterVars = new ArrayList<String>();
            rightOuterVars = new ArrayList<String>();
            sharedVars = getSharedVars();
            leftOuterVars.addAll(leftOp.getSchema());
            leftOuterVars.removeAll(sharedVars);
            rightOuterVars.addAll(rightOp.getSchema());
            rightOuterVars.removeAll(sharedVars);
            String u1 = "u1 = FOREACH " + leftOp.getResultName() + " GENERATE ";
            String u2 = "u2 = FOREACH " + rightOp.getResultName() + " GENERATE ";
            boolean first = true;
            for (int i=0; i<sharedVars.size(); i++) {
                u1 += (first ? "": ", ") + sharedVars.get(i);
                u2 += (first ? "": ", ") + sharedVars.get(i);
                first = false;
            }
            for (int i=0; i<leftOuterVars.size(); i++) {
                u1 += (first ? "": ", ") + leftOuterVars.get(i);
                u2 += (first ? "": ", ") + "null AS " + leftOuterVars.get(i);
                first = false;
            }
            for (int i=0; i<rightOuterVars.size(); i++) {
                u1 += (first ? "": ", ") + "null AS " + rightOuterVars.get(i);
                u2 += (first ? "": ", ") + rightOuterVars.get(i);
                first = false;
            }
            union += u1 + " ;\n" + u2 + " ;\n";
            union += resultName + " = UNION " + "u1, u2";
            resultSchema.addAll(sharedVars);
            resultSchema.addAll(leftOuterVars);
            resultSchema.addAll(rightOuterVars);
            varsWithNulls.addAll(leftOuterVars);
            varsWithNulls.addAll(rightOuterVars);
        }
        union += " ;\n";
        return union;
    }


    @Override
    public void visit(PigOpVisitor pigOpVisitor) {
        pigOpVisitor.visit(this);
    }

}
