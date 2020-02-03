package script;

import Util.MyUtils;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteArrayDataInput;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author yshhuang@foxmail.com
 * @date 2019-11-19 17:40
 */
public class NormL2Script implements ImageRevirevalScript {
    @Override
    public ScoreScript.LeafFactory getScript(Map<String, Object> p,SearchLookup lookup) {
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
                field = p.get("field").toString();
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

                        double sum = 0;
                        for (int i = 0; i != inputVectorSize; i++) {
                            sum += Math.pow(inputVector[i] - doubles[i],2);
                        }
                        double s = 1 - Math.sqrt(sum);
                        return s < 0 ? 0 : s;
                    }
                };
            }

            @Override
            public boolean needs_score() {
                return false;
            }
        };
    }
}
