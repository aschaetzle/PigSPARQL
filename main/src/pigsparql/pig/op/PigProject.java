package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
import java.util.Iterator;
import java.util.List;
import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;

/**
 *
 * @author Alexander Schaetzle
 */
public class PigProject extends PigOp1 {

    private OpProject opProject;


    public PigProject(OpProject _opProject, PigOp _subOp, PrefixMapping _prefixes) {
        super(_subOp, _prefixes);
        opProject = _opProject;
        resultName = Tags.PROJECT;
    }


    @Override
    public String getName() {
        return Tags.PIG_PROJECT;
    }


    @Override
    public String translate(String _resultName) {
        if(subOp.getSchema().size() == opProject.getVars().size()) {
            resultName = subOp.getResultName();
            resultSchema.addAll(subOp.getSchema());
            return "";
        }

        resultName = _resultName;
        varsWithNulls.addAll(subOp.getVarsWithNulls());
        String projection = "-- " + Tags.PROJECT + "\n";

        projection += generateForeach();

        return projection;
    }

    private String generateForeach() {
        String foreach = resultName + " = FOREACH ";
        foreach += subOp.getResultName() + " GENERATE ";
        
        List<Var> selectedVars = opProject.getVars();
        Iterator<Var> varIterator = selectedVars.iterator();
        String varName;
        varName = varIterator.next().getVarName();
        foreach += varName;
        resultSchema.add(varName);
        while (varIterator.hasNext()) {
            varName = varIterator.next().getVarName();
            foreach += ", " + varName;
            resultSchema.add(varName);
        }
        foreach += " ;\n";
        return foreach;
    }


    @Override
    public void visit(PigOpVisitor pigOpVisitor) {
        pigOpVisitor.visit(this);
    }

}
