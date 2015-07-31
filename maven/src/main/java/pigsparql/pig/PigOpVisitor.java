package pigsparql.pig;

import pigsparql.pig.op.PigBGP;
import pigsparql.pig.op.PigConditional;
import pigsparql.pig.op.PigDistinct;
import pigsparql.pig.op.PigFilter;
import pigsparql.pig.op.PigJoin;
import pigsparql.pig.op.PigLeftJoin;
import pigsparql.pig.op.PigOrder;
import pigsparql.pig.op.PigProject;
import pigsparql.pig.op.PigReduced;
import pigsparql.pig.op.PigSequence;
import pigsparql.pig.op.PigSlice;
import pigsparql.pig.op.PigUnion;

/**
 *
 * @author Alexander Schaetzle
 */
public interface PigOpVisitor {
    
    // Operators
    public void visit(PigBGP pigBGP);
    public void visit(PigFilter pigFilter);
    public void visit(PigJoin pigJoin);
    public void visit(PigSequence pigSequence);
    public void visit(PigLeftJoin pigLeftJoin);
    public void visit(PigConditional pigConditional);
    public void visit(PigUnion pigUnion);

    // Solution Modifier
    public void visit(PigProject pigProject);
    public void visit(PigDistinct pigDistinct);
    public void visit(PigReduced pigReduced);
    public void visit(PigOrder pigOrder);
    public void visit(PigSlice pigSlice);

}
