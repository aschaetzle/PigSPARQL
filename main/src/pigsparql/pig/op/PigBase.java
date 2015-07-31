package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import java.util.ArrayList;

/**
 *
 * @author Alexander Schaetzle
 */
public abstract class PigBase implements PigOp {

    protected PrefixMapping prefixes;
    protected boolean expandPrefixes;
    protected String resultName;
    protected ArrayList<String> resultSchema;
    protected ArrayList<String> varsWithNulls;

    
    @Override
    public ArrayList<String> getSchema() {
        return resultSchema;
    }

    @Override
    public ArrayList<String> getVarsWithNulls() {
        return varsWithNulls;
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public void setResultName(String _resultName) {
        resultName = _resultName;
    }

    @Override
    public boolean getExpandMode() {
        return expandPrefixes;
    }

    @Override
    public void setExpandMode(boolean _expandPrefixes) {
        expandPrefixes = _expandPrefixes;
    }

    
    @Override
    public String translate() {
        if(resultName != null) {
            return translate(resultName);
        }
        else return null;
    }

    protected String toArgumentList(ArrayList<String> list) {
        if(list == null || list.isEmpty()) return null;
        String argList = "";
        if(list.size() == 1) return list.get(0);
        else {
            argList += "(" + list.get(0);
            for (int i=1; i<list.size(); i++) {
                argList += ", " + list.get(i);
            }
            argList += ")";
        }
        return argList;
    }

    protected boolean checkForNullJoin(ArrayList<String> sharedVars) {
        ArrayList<String> nullVars = new ArrayList<String>();
        nullVars.addAll(sharedVars);
        nullVars.retainAll(varsWithNulls);
        if (nullVars.isEmpty())
            return false;
        else
            return true;
    }

}
