package pigsparql.run;


import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;


public class QueryEvaluator
{
    public static void main(String []args)
    {
        // Parse
        //Query query = QueryFactory.read("file:C:\\SVN\\PigSPARQL_main\\queries\\q8.sparql") ;
        Query query = QueryFactory.read("file:queries/SP2Bench/q8mod.sparql") ;
        //System.out.println(query) ;
        
        // Generate algebra
        Op op = Algebra.compile(query) ;
        op = Algebra.optimize(op) ;
        //System.out.println(op) ;

        // Print Algebra Using SSE
        //PrintUtils.printOp(query, true);
        //System.out.println();

        String dftGraphURI = "file:datasets/SP2BEnch/dblp25M.n3" ;
        //String dftGraphURI = "file:D:\\ZerProf\\Uni\\Master\\Masterarbeit\\sp2b\\bin\\dblp50K.n3" ;
        Dataset dataset = DatasetFactory.create(dftGraphURI);
        
        // Execute it.
        QueryIterator qIter = Algebra.exec(op, dataset) ;
        
        // Results
        int results = 0;
        for ( ; qIter.hasNext() ; )
        {
            Binding b = qIter.nextBinding() ;
            results++;
            System.out.println(b) ;
        }
        qIter.close() ;
        System.out.println("# solution mappings: "+results);
    }
}
