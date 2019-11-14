import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteArrayDataInput;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author yshhuang@foxmail.com
 * @date 2019-08-26 14:26
 */
public class VectorDistanceEngine implements ScriptEngine {

    //The normalized vector score from the query
    double queryVectorNorm;

    /**
     * The language name used in the script APIs to refer to this scripting backend.
     */
    @Override
    public String getType() {
        return VectorDistanceConfig.ENGINE_TYPE;
    }

    /**
     * Compiles a script.
     *
     * @param name    the name of the script. {@code null} if it is anonymous (inline). For a stored script, its the identifier.
     * @param code    actual source of the script
     * @param context the context this script will be used for
     * @param params  compile-time parameters (such as flags to the compiler)
     * @return A compiled script of the FactoryType from {@link ScriptContext}
     */
    @Override
    public <T> T compile(String name,String code,ScriptContext<T> context,Map<String, String> params) {
        if (context.equals(ScoreScript.CONTEXT) == false) {
            throw new IllegalArgumentException(getType() + " scripts cannot be used for context [" + context.name + "]");
        }
        // we use the script "source" as the script identifier
        if (!VectorDistanceConfig.SCRIPT_SOURCE.equals(code)) {
            throw new IllegalArgumentException("Unknown script name " + code);
        }

        ScoreScript.Factory factory = (p,lookup) -> {
            String method = p.get("method").toString();
            switch (method) {
                case "cosine":
                    return cosine(p,lookup);
                case "hamming":
                    return hamming(p,lookup);
                default:
                    return null;
            }
        };
        return context.factoryClazz.cast(factory);
    }

    private ScoreScript.LeafFactory hamming(Map<String, Object> p,SearchLookup lookup) {
        return new ScoreScript.LeafFactory() {
            // The field to compare against
            final String field;
            //The query embedded value
            final String value;

            {
                if (p.containsKey("field") == false) {
                    throw new IllegalArgumentException("Missing parameter [field]");
                }
                //Get the field value from the query
                field = p.get("field").toString();
                //Get the query value embedding
                value = p.get("value").toString();

                if (value == null) {
                    throw new IllegalArgumentException("Must have 'value' as a parameter");
                }
            }

            @Override
            public ScoreScript newInstance(LeafReaderContext context) throws IOException {
                return new ScoreScript(p,lookup,context) {
                    Boolean is_value = false;
                    // Use Lucene LeafReadContext to access binary values directly.
                    BinaryDocValues accessor = context.reader().getBinaryDocValues(field);

                    @Override
                    public void setDocument(int docId) {
                        try {
                            accessor.advanceExact(docId);
                            is_value = true;
                        } catch (Exception e) {
                            e.printStackTrace();
                            is_value = false;
                        }
                    }

                    @Override
                    public double execute() {
                        //If there is no field value return 0 rather than fail.
                        if (!is_value) return 0.0d;

                        final int inputVectorSize = value.length();
                        final byte[] bytes;
                        try {
                            bytes = accessor.binaryValue().bytes;
                        } catch (IOException e) {
                            return 0d;
                        }
                        final ByteArrayDataInput docVector = new ByteArrayDataInput(bytes);

                        docVector.readVInt();

                        final int docVectorLength = docVector.readVInt(); // returns the number of bytes to read
                        if (docVectorLength != inputVectorSize * 8) {
                            return 0d;
                        }
                        final int position = docVector.getPosition();
                        final CharBuffer doubleBuffer =
                                ByteBuffer.wrap(bytes,position,docVectorLength).asCharBuffer();

                        final char[] doubles = new char[inputVectorSize];
                        doubleBuffer.get(doubles);
                        int counter = 0;
                        for (int k = 0; k < value.length(); k++) {
                            if (value.charAt(k) != doubles[k]) {
                                counter++;
                            }
                        }
                        return 1 - 1.0 * counter / value.length();
                    }
                };
            }

            @Override
            public boolean needs_score() {
                return false;
            }
        };
    }

    public ScoreScript.LeafFactory cosine(Map<String, Object> p,SearchLookup lookup) {
        return new ScoreScript.LeafFactory() {
            // The field to compare against
            final String field;
            //The query embedded value
            final Object value;
            //The final comma delimited value representation of the query value
            double[] inputVector;

            {
                if (p.containsKey("field") == false) {
                    throw new IllegalArgumentException("Missing parameter [field]");
                }
                //Get the field value from the query
                field = p.get("field").toString();
                //Get the query value embedding
                value = p.get("value");

                //Determine if raw comma-delimited value or embedding was passed
                if (value != null) {
                    final ArrayList<Double> tmp = (ArrayList<Double>) value;
                    inputVector = new double[tmp.size()];
                    for (int i = 0; i < inputVector.length; i++) {
                        inputVector[i] = tmp.get(i);
                    }
                } else {
                    throw new IllegalArgumentException("Must have 'value' as a parameter");
                }

                queryVectorNorm = 0d;
                // compute query inputVector norm once
                for (double v : inputVector) {
                    queryVectorNorm += Math.pow(v,2.0);
                }
            }

            @Override
            public ScoreScript newInstance(LeafReaderContext context) throws IOException {
                return new ScoreScript(p,lookup,context) {
                    Boolean is_value = false;
                    // Use Lucene LeafReadContext to access binary values directly.
                    BinaryDocValues accessor = context.reader().getBinaryDocValues(field);

                    @Override
                    public void setDocument(int docId) {
                        try {
                            accessor.advanceExact(docId);
                            is_value = true;
                        } catch (Exception e) {
                            e.printStackTrace();
                            is_value = false;
                        }
                    }

                    @Override
                    public double execute() {
                        //If there is no field value return 0 rather than fail.
                        if (!is_value) return 0.0d;

                        final int inputVectorSize = inputVector.length;
                        final byte[] bytes;
                        try {
                            bytes = accessor.binaryValue().bytes;
                        } catch (IOException e) {
                            return 0d;
                        }
                        final ByteArrayDataInput docVector = new ByteArrayDataInput(bytes);

                        docVector.readVInt();

                        final int docVectorLength = docVector.readVInt(); // returns the number of bytes to read
                        if (docVectorLength != inputVectorSize * 8) {
                            return 0d;
                        }
                        final int position = docVector.getPosition();
                        final DoubleBuffer doubleBuffer =
                                ByteBuffer.wrap(bytes,position,docVectorLength).asDoubleBuffer();

                        final double[] doubles = new double[inputVectorSize];
                        doubleBuffer.get(doubles);
                        double docVectorNorm = 0d;
                        double score = 0d;

                        //calculate dot product of document value and query value
                        for (int i = 0; i < inputVectorSize; i++) {
                            score += doubles[i] * inputVector[i];
                            docVectorNorm += Math.pow(doubles[i],2.0);
                        }
                        if (docVectorNorm == 0 || queryVectorNorm == 0) return 0d;
                        score = score / (Math.sqrt(docVectorNorm) * Math.sqrt(queryVectorNorm));
                        return score;
                    }
                };
            }

            @Override
            public boolean needs_score() {
                return false;
            }
        };
    }

    @Override
    public void close() {
        // optionally close resources
    }
}
