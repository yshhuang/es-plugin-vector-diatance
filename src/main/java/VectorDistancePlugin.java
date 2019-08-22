import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteArrayDataInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * @author yshhuang@foxmail.com
 * @date 2019-08-22 15:29
 */
public class VectorDistancePlugin extends Plugin implements ScriptPlugin {
    @Override
    public ScriptEngine getScriptEngine(Settings settings,Collection<ScriptContext<?>> contexts) {
        return new VectorDistanceEngine();
    }

    private static class VectorDistanceEngine implements ScriptEngine {
        //
        //The normalized vector score from the query
        //
        double queryVectorNorm;

        @Override
        public String getType() {
            return VectorDistanceConfig.ENGINE_TYPE;
        }

        @Override
        public <T> T compile(String scriptName,String scriptSource,ScriptContext<T> context,Map<String, String> params) {
            if (context.equals(ScoreScript.CONTEXT) == false) {
                throw new IllegalArgumentException(getType() + " scripts cannot be used for context [" + context.name + "]");
            }
            // we use the script "source" as the script identifier
            if (VectorDistanceConfig.SCRIPT_SOURCE.equals(scriptSource)) {
                ScoreScript.Factory factory = (p,lookup) -> new ScoreScript.LeafFactory() {
                    // The field to compare against
                    final String field;
                    //Whether this search should be cosine or dot product
                    final Boolean cosine;
                    //The query embedded vector
                    final Object vector;
                    Boolean exclude;
                    //The final comma delimited vector representation of the query vector
                    double[] inputVector;

                    {
                        if (p.containsKey("field") == false) {
                            throw new IllegalArgumentException("Missing parameter [field]");
                        }

                        //Determine if cosine
                        final Object cosineBool = p.get("cosine");
                        cosine = cosineBool != null ? (boolean) cosineBool : true;
                        //Get the field value from the query
                        field = p.get("field").toString();
                        final Object excludeBool = p.get("exclude");
                        exclude = excludeBool != null ? (boolean) cosineBool : true;
                        //Get the query vector embedding
                        vector = p.get("vector");

                        //Determine if raw comma-delimited vector or embedding was passed
                        if (vector != null) {
                            final ArrayList<Double> tmp = (ArrayList<Double>) vector;
                            inputVector = new double[tmp.size()];
                            for (int i = 0; i < inputVector.length; i++) {
                                inputVector[i] = tmp.get(i);
                            }
                        } else {

                            throw new IllegalArgumentException("Must have 'vector' or 'encoded_vector' as a parameter");
                        }
                        //If cosine calculate the query vec norm
                        if (cosine) {
                            queryVectorNorm = 0d;
                            // compute query inputVector norm once
                            for (double v : inputVector) {
                                queryVectorNorm += Math.pow(v,2.0);
                            }
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
                                // advance has undefined behavior calling with a docid <= its current docid
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

                                //calculate dot product of document vector and query vector
                                for (int i = 0; i < inputVectorSize; i++) {
                                    score += doubles[i] * inputVector[i];
                                    if (cosine) {
                                        docVectorNorm += Math.pow(doubles[i],2.0);
                                    }
                                }

                                //If cosine, calcluate cosine score
                                if (cosine) {
                                    if (docVectorNorm == 0 || queryVectorNorm == 0) return 0d;
                                    score = score / (Math.sqrt(docVectorNorm) * Math.sqrt(queryVectorNorm));
                                }
                                return score;
                            }
                        };
                    }

                    @Override
                    public boolean needs_score() {
                        return false;
                    }
                };
                return context.factoryClazz.cast(factory);
            }
            throw new IllegalArgumentException("Unknown script name " + scriptSource);
        }

        @Override
        public void close() {
            // optionally close resources
        }
    }
}
