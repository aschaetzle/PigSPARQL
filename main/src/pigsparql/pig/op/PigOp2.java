package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import java.util.ArrayList;

/**
 *
 * @author Alexander Schaetzle
 */
public abstract class PigOp2 extends PigBase {

    protected PigOp leftOp, rightOp;

    
    protected PigOp2(PigOp _leftOp, PigOp _rightOp, PrefixMapping _prefixes) {
        leftOp = _leftOp;
        rightOp = _rightOp;
        prefixes = _prefixes;
        resultSchema = new ArrayList<String>();
        varsWithNulls = new ArrayList<String>();
    }

    public PigOp getLeft() {
        return leftOp;
    }

    public PigOp getRight() {
        return rightOp;
    }


    protected ArrayList<String> getSharedVars() {
        ArrayList<String> leftOpSchema = leftOp.getSchema();
        ArrayList<String> rightOpSchema = rightOp.getSchema();
        ArrayList<String> sharedVars = new ArrayList<String>();

        String currentVar;
        for(int i=0; i<leftOpSchema.size(); i++) {
            currentVar = leftOpSchema.get(i);
            if(rightOpSchema.contains(currentVar)) {
                sharedVars.add(currentVar);
            }
        }
        return sharedVars;
    }


    protected String generateJoinProjection(String inputName, String resultName) {
        String foreach = resultName + " = FOREACH " + inputName + " GENERATE ";
        ArrayList<String> leftOpSchema = leftOp.getSchema();
        ArrayList<String> rightOpSchema = rightOp.getSchema();
        String leftName = leftOp.getResultName();
        String rightName = rightOp.getResultName();
        String field = leftOpSchema.get(0);
        // schema of left subOp completely
        foreach += leftName + "::" + field + " AS " + field;
        resultSchema.add(field);
        for (int i=1; i<leftOpSchema.size(); i++) {
            field = leftOpSchema.get(i);
            foreach += ", " + leftName + "::" + field + " AS " + field;
            resultSchema.add(field);
        }
        // schema of right subOp without shared variables
        for (int i=0; i<rightOpSchema.size(); i++) {
            field = rightOpSchema.get(i);
            if (!leftOpSchema.contains(field)) {
                foreach += ", " + rightName + "::" + field + " AS " + field;
                resultSchema.add(field);
            }
        }
        foreach += " ;\n";
        return foreach;
    }

}
