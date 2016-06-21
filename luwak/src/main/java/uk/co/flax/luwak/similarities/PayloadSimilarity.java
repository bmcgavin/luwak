package uk.co.flax.luwak.similarities;

import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.analysis.payloads.PayloadHelper;

public class PayloadSimilarity extends DefaultSimilarity {
    public PayloadSimilarity() {}
    
    @Override
    public float scorePayload(int doc, int start, int end, BytesRef payload) {
      if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Hello I am in new magic scorePayload");
      //return 1;
      return PayloadHelper.decodeFloat(payload.bytes);//we can ignore length here, because we know it is encoded as 4 bytes
    }
}
