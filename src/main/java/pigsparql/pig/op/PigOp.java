package pigsparql.pig.op;

import java.util.ArrayList;
import pigsparql.pig.PigOpVisitor;

/**
 *
 * @author Alexander Schaetzle
 */
public interface PigOp {

    public String getName();
    public String translate();
    public String translate(String _resultName);
    public ArrayList<String> getSchema();
    public ArrayList<String> getVarsWithNulls();
    public String getResultName();
    public void setResultName(String _resultName);
    public void visit(PigOpVisitor pigOpVisitor);

}
