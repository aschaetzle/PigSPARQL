package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;

import java.util.ArrayList;

/**
 *
 * @author Alexander Schaetzle
 */
public abstract class PigOpBase implements PigOp {

    protected PrefixMapping prefixes;
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
    public String translate() {
        if(resultName != null) {
            return translate(resultName);
        }
        else return null;
    }
    
    
    protected ArrayList<String> getSharedVars(ArrayList<String> leftSchema, ArrayList<String> rightSchema) {
		ArrayList<String> sharedVars = new ArrayList<String>();
		for (int i = 0; i < rightSchema.size(); i++) {
			if (leftSchema.contains(rightSchema.get(i)))
				sharedVars.add(rightSchema.get(i));
		}
		return sharedVars;
	}
	
	
	protected void addToResultSchema(ArrayList<String> relSchema) {
		String field;
		for (int i = 0; i < relSchema.size(); i++) {
			field = relSchema.get(i);
			if (!resultSchema.contains(field)) {
				resultSchema.add(field);
			}
		}
	}
	
	
	protected String addToProjectArgs(String projection, ArrayList<String> currentSchema, String relName, ArrayList<String> relSchema) {
		String field;
		boolean first = projection.isEmpty();
		for (int i = 0; i < relSchema.size(); i++) {
			field = relSchema.get(i);
			if (currentSchema == null || !currentSchema.contains(field)) {
				projection += (first ? "" : ", ") + relName + "::" + field + " AS " + field;
				first = false;
			}
		}
		return projection;
	}
	

    protected String toArgumentList(ArrayList<String> list) {
        if(list == null || list.isEmpty()) return null;
        String argList = "";
        if(list.size() == 1) return list.get(0);
        else {
            argList += "(" + list.get(0);
            for (int i=1; i<list.size(); i++) {
                argList += "," + list.get(i);
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
