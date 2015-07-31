package pigsparql.pig;

import org.apache.log4j.Logger;
import pigsparql.pig.op.PigBGP;
import pigsparql.pig.op.PigConditional;
import pigsparql.pig.op.PigDistinct;
import pigsparql.pig.op.PigFilter;
import pigsparql.pig.op.PigJoin;
import pigsparql.pig.op.PigLeftJoin;
import pigsparql.pig.op.PigOp;
import pigsparql.pig.op.PigOp1;
import pigsparql.pig.op.PigOp2;
import pigsparql.pig.op.PigOpN;
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
public abstract class PigOpVisitorByType implements PigOpVisitor {
    
    // Define a static logger variable so that it references the corresponding Logger instance
    protected static Logger logger = Logger.getLogger(PigOpVisitor.class);
    

    /**
     * Operators with no sub operators
     * @param op 
     */
    protected abstract void visit0(PigOp op);

    /**
     * Operators with 1 sub operator
     * @param op 
     */ 
    protected abstract void visit1(PigOp1 op);

    /**
     * Operators with 2 sub operators
     * @param op 
     */
    protected abstract void visit2(PigOp2 op);

    /**
     * Operators with N sub operators
     * @param op 
     */
    protected abstract void visitN(PigOpN op);
    

    // Declare basic visit methods as final such that derived classes cannot override it
    // OPERATORS
    @Override
    public final void visit(PigBGP pigBGP) {
        visit0(pigBGP);
    }
    
    @Override
    public final void visit(PigFilter pigFilter) {
        visit1(pigFilter);
    }

    @Override
    public final void visit(PigJoin pigJoin) {
        visit2(pigJoin);
    }

    @Override
    public final void visit(PigSequence pigSequence) {
        visitN(pigSequence);
    }

    @Override
    public final void visit(PigLeftJoin pigLeftJoin) {
        visit2(pigLeftJoin);
    }

    @Override
    public final void visit(PigConditional pigConditional) {
        visit2(pigConditional);
    }

    @Override
    public final void visit(PigUnion pigUnion) {
        visit2(pigUnion);
    }

    // SOLUTION MODIFIERS
    @Override
    public final void visit(PigProject pigProject) {
        visit1(pigProject);
    }

    @Override
    public final void visit(PigDistinct pigDistinct) {
        visit1(pigDistinct);
    }

    @Override
    public final void visit(PigReduced pigReduced) {
        visit1(pigReduced);
    }

    @Override
    public final void visit(PigOrder pigOrder) {
        visit1(pigOrder);
    }

    @Override
    public final void visit(PigSlice pigSlice) {
        visit1(pigSlice);
    }

}
