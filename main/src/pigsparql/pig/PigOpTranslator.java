package pigsparql.pig;

import pigsparql.pig.op.PigBGP;
import pigsparql.pig.op.PigConditional;
import pigsparql.pig.op.PigDistinct;
import pigsparql.pig.op.PigFilter;
import pigsparql.pig.op.PigJoin;
import pigsparql.pig.op.PigLeftJoin;
import pigsparql.pig.op.PigOp;
import pigsparql.pig.op.PigOrder;
import pigsparql.pig.op.PigProject;
import pigsparql.pig.op.PigReduced;
import pigsparql.pig.op.PigSequence;
import pigsparql.pig.op.PigSlice;
import pigsparql.pig.op.PigUnion;

/**
 * Translates a PigOp Tree into a corresponding Pig Latin program.
 * It walks through the PigOp Tree bottom up and generates the commands for every operator.
 * A SPARQL Algebra Tree must be translated into a corresponding PigOp Tree
 * before using the PigOpTranslator.
 *
 * @see pigsparql.sparql.AlgebraTransformer
 */
public class PigOpTranslator extends PigOpVisitorBase {

    // Expand prefixes or not
    private boolean expandPrefixes;
    // Count the occurences of an operator
    private int countBGP, countJoin, countLeftJoin, countUnion, countSequence, countFilter;
    // Output Pig Script
    private String pigScript;


    /**
     * Constructor of class PigOpTranslator.
     */
    public PigOpTranslator() {
        countBGP = 0;
        countJoin = 0;
        countLeftJoin = 0;
        countUnion = 0;
        countSequence = 0;
        countFilter = 0;
        pigScript = "";
    }

    /**
     * Translates a PigOp Tree into a corresponding Pig Latin program.
     *
     * @param op Root of the PigOp Tree
     * @param _expandPrefixes Expand prefixes used in the original query or not
     * @return Pig Latin program
     */
    public String translate(PigOp op, boolean _expandPrefixes) {
        expandPrefixes = _expandPrefixes;
        //Walk through the tree bottom up
        PigOpWalker.walkBottomUp(this, op);
        return pigScript;
    }

    /**
     * Append commands to the end of the Pig Latin program.
     *
     * @param block String to be appended
     */
    private void appendToScript(String block) {
        if (!block.equals("")) {
            pigScript += block + "\n";
        }
    }

    

    /**
     * Translates a BGP into corresponding Pig Latin commands.
     *
     * @param pigBGP BGP in the PigOp Tree
     */
    @Override
    public void visit(PigBGP pigBGP) {
        countBGP++;
        pigBGP.setExpandMode(expandPrefixes);
        String bgp = pigBGP.translate(Tags.BGP + countBGP);
        appendToScript(bgp);
    }

    /**
     * Translates a FILTER into corresponding Pig Latin commands.
     *
     * @param pigFilter FILTER in the PigOp Tree
     */
    @Override
    public void visit(PigFilter pigFilter) {
        countFilter++;
        String filter = pigFilter.translate(Tags.FILTER + countFilter);
        appendToScript(filter);
    }

    /**
     * Translates a JOIN into corresponding Pig Latin commands.
     *
     * @param pigJoin JOIN in the PigOp Tree
     */
    @Override
    public void visit(PigJoin pigJoin) {
        countJoin++;
        String join = pigJoin.translate(Tags.JOIN + countJoin);
        appendToScript(join);
    }

    /**
     * Translates a sequence of JOINs into corresponding Pig Latin commands.
     *
     * @param pigSequence JOIN sequence in the PigOp Tree
     */
    @Override
    public void visit(PigSequence pigSequence) {
        countSequence++;
        String join = pigSequence.translate(Tags.SEQUENCE + countSequence);
        appendToScript(join);
    }

    /**
     * Translates a LEFTJOIN into corresponding Pig Latin commands.
     *
     * @param pigLeftJoin LEFTJOIN in the PigOp Tree
     */
    @Override
    public void visit(PigLeftJoin pigLeftJoin) {
        countLeftJoin++;
        String leftJoin = pigLeftJoin.translate(Tags.LEFT_JOIN + countLeftJoin);
        appendToScript(leftJoin);
    }

    /**
     * Translates a LEFTJOIN without Filter (Conditional) into corresponding Pig Latin commands.
     *
     * @param pigConditional LEFTJOIN without Filter (Conditional) in the PigOp Tree
     */
    @Override
    public void visit(PigConditional pigConditional) {
        countLeftJoin++;
        String leftJoin = pigConditional.translate(Tags.CONDITIONAL + countLeftJoin);
        appendToScript(leftJoin);
    }

    /**
     * Translates a UNION into corresponding Pig Latin commands.
     *
     * @param pigUnion UNION in the PigOp Tree
     */
    @Override
    public void visit(PigUnion pigUnion) {
        countUnion++;
        String union = pigUnion.translate(Tags.UNION + countUnion);
        appendToScript(union);
    }

    /**
     * Translates a PROJECT into corresponding Pig Latin commands.
     *
     * @param pigProject PROJECT in the PigOp Tree
     */
    @Override
    public void visit(PigProject pigProject) {
        String projection = pigProject.translate(Tags.PROJECT);
        appendToScript(projection);
    }

    /**
     * Translates a DISTINCT into corresponding Pig Latin commands.
     *
     * @param pigDistinct Distinct in the PigOp Tree
     */
    @Override
    public void visit(PigDistinct pigDistinct) {
        String distinct = pigDistinct.translate(Tags.DISTINCT);
        appendToScript(distinct);
    }

    /**
     * Translates a REDUCE into corresponding Pig Latin commands.
     *
     * @param pigReduced REDUCE in the PigOp Tree
     */
    @Override
    public void visit(PigReduced pigReduced) {
        String reduced = pigReduced.translate(Tags.REDUCED);
        appendToScript(reduced);
    }

    /**
     * Translates an ORDER into corresponding Pig Latin commands.
     *
     * @param pigOrder ORDER in the PigOp Tree
     */
    @Override
    public void visit(PigOrder pigOrder) {
        String order = pigOrder.translate(Tags.ORDER);
        appendToScript(order);
    }

    /**
     * Translates a SLICE into corresponding Pig Latin commands.
     *
     * @param pigSlice SLICE in the PigOp Tree
     */
    @Override
    public void visit(PigSlice pigSlice) {
        String slice = pigSlice.translate(Tags.SLICE);
        appendToScript(slice);
    }

}
