package pigsparql.pig;

import org.apache.log4j.Logger;
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
public class PigOpVisitorBase implements PigOpVisitor {
    
    // Define a static logger variable so that it references the corresponding Logger instance
    protected static Logger logger = Logger.getLogger(PigOpVisitor.class);

    // OPERATORS
    @Override
    public void visit(PigBGP pigBGP) {
        logger.error("BGP not supported yet!");
        throw new UnsupportedOperationException("BGP not supported yet!");
    }

    @Override
    public void visit(PigFilter pigFilter) {
        logger.error("FILTER not supported yet!");
        throw new UnsupportedOperationException("FILTER not supported yet!");
    }

    @Override
    public void visit(PigJoin pigJoin) {
        logger.error("JOIN not supported yet!");
        throw new UnsupportedOperationException("JOIN not supported yet!");
    }

    @Override
    public void visit(PigSequence pigSequence) {
        logger.error("SEQUENCE not supported yet!");
        throw new UnsupportedOperationException("SEQUENCE not supported yet!");
    }

    @Override
    public void visit(PigLeftJoin pigLeftJoin) {
        logger.error("LEFTJOIN not supported yet!");
        throw new UnsupportedOperationException("LEFTJOIN not supported yet!");
    }

    @Override
    public void visit(PigConditional pigConditional) {
        logger.error("CONDITIONAL not supported yet!");
        throw new UnsupportedOperationException("CONDITIONAL not supported yet!");
    }

    @Override
    public void visit(PigUnion pigUnion) {
        logger.error("UNION not supported yet!");
        throw new UnsupportedOperationException("UNION not supported yet!");
    }

    // SOLUTION MODIFIERS
    @Override
    public void visit(PigProject pigProject) {
        logger.error("PROJECT not supported yet!");
        throw new UnsupportedOperationException("PROJECT not supported yet!");
    }

    @Override
    public void visit(PigDistinct pigDistinct) {
        logger.error("DISTINCT not supported yet!");
        throw new UnsupportedOperationException("DISTINCT not supported yet!");
    }

    @Override
    public void visit(PigReduced pigReduced) {
        logger.error("REDUCED not supported yet!");
        throw new UnsupportedOperationException("REDUCED not supported yet!");
    }

    @Override
    public void visit(PigOrder pigOrder) {
        logger.error("ORDER not supported yet!");
        throw new UnsupportedOperationException("ORDER not supported yet!");
    }

    @Override
    public void visit(PigSlice pigSlice) {
        logger.error("SLICE not supported yet!");
        throw new UnsupportedOperationException("SLICE not supported yet!");
    }

}
