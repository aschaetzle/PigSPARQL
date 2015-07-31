package pigsparql.udf;

import java.io.IOException;
import org.apache.pig.builtin.Utf8StorageConverter;

/**
 * Converts UTF8 encoded data and typed RDF Literals into Pig Datatypes.
 * Supported typed literals: integer (^^xsd:integer), float (^^xsd:float), double (^^xsd:double)
 *
 * @author Alexander Schaetzle
 * @version 1.0
 */
public class RDFStorageConverter extends Utf8StorageConverter {

    /**
     * Defined in Interface LoadCaster of PIG.
     * Typed RDF Literals of type xsd:integer are converted into UTF8 encoded integers.
     * The conversion for the UTF8 encoded data is done by class Utf8StorageConverter.
     *
     * @see org.apache.pig.LoadCaster#bytesToInteger(byte[])
     * @see org.apache.pig.builtin.Utf8StorageConverter
     *
     * @param b
     * @return parsed Integer
     * @throws IOException
     */
    @Override
    public Integer bytesToInteger(byte[] b) throws IOException {
        if(b == null)
            return null;
        String s = new String(b);
        // check if string is a typed RDF Literal with type integer
        if(s.endsWith("^^xsd:integer") || s.endsWith("^^<http://www.w3.org/2001/XMLSchema#integer>")
                || s.endsWith("^^xsd:int") || s.endsWith("^^<http://www.w3.org/2001/XMLSchema#int>")) {
            // extract Literal value (value is UTF8 encoded)
            s = s.substring(1, s.lastIndexOf("\""));
        }
        return super.bytesToInteger(s.getBytes());
    }

    /**
     * Defined in Interface LoadCaster of PIG.
     * Typed RDF Literals of type xsd:double are converted into UTF8 encoded doubles.
     * The conversion for the UTF8 encoded data is done by class Utf8StorageConverter.
     *
     * @see org.apache.pig.LoadCaster#bytesToDouble(byte[])
     * @see org.apache.pig.builtin.Utf8StorageConverter
     *
     * @param b
     * @return parsed Double
     */
    @Override
    public Double bytesToDouble(byte[] b) {
        if(b == null)
            return null;
        String s = new String(b);
        // check if string is a typed RDF Literal with type double
        if(s.endsWith("^^xsd:double") || s.endsWith("^^<http://www.w3.org/2001/XMLSchema#double>")) {
            // extract Literal value (value is UTF8 encoded)
            s = s.substring(1, s.lastIndexOf("\""));
        }
        return super.bytesToDouble(s.getBytes());
    }

    /**
     * Defined in Interface LoadCaster of PIG.
     * Typed RDF Literals of type xsd:float are converted into UTF8 encoded floats.
     * The conversion for the UTF8 encoded data is done by class Utf8StorageConverter.
     *
     * @see org.apache.pig.LoadCaster#bytesToFloat(byte[])
     * @see org.apache.pig.builtin.Utf8StorageConverter
     *
     * @param b
     * @return parsed Float
     * @throws IOException
     */
    @Override
    public Float bytesToFloat(byte[] b) throws IOException {
        if(b == null)
            return null;
        String s = new String(b);
        // check if string is a typed RDF Literal with type float
        if(s.endsWith("^^xsd:float") || s.endsWith("^^<http://www.w3.org/2001/XMLSchema#float>")) {
            // extract Literal value (value is UTF8 encoded)
            s = s.substring(1, s.lastIndexOf("\""));
        }
        return super.bytesToFloat(s.getBytes());
    }

}