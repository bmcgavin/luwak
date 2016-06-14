package uk.co.flax.luwak.similarities;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Attribute;

import java.util.Iterator;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class StartBoostSimilarity extends DefaultSimilarity {

    private CharTermAttribute termAtt;
    
    @Override
    public float lengthNorm(FieldInvertState state) {
        // simply return the field's configured boost value
        // instead of also factoring in the field's length
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("HELLO I AM IN lengthNorm");
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getAttributeSource : " + state.getAttributeSource());
        if (state.getAttributeSource() != null) {
            Iterator<Class<? extends Attribute>> it = state.getAttributeSource().getAttributeClassesIterator();
            Object O = null;
            while (it.hasNext()) {
                O = it.next();
                if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getAttributeSource.iterator : " + O);
            }
            termAtt = state.getAttributeSource().getAttribute(CharTermAttribute.class);
            if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("termAtt.length() : " + termAtt.length());
        }
        
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getBoost : " + state.getBoost());
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getLength : " + state.getLength());
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getMaxTermFrequency : " + state.getMaxTermFrequency());
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getName : " + state.getName());
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getNumOverlap : " + state.getNumOverlap());
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getOffset : " + state.getOffset());
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getPosition : " + state.getPosition());
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getUniqueTermCount : " + state.getUniqueTermCount());
        return state.getBoost();
    }

    @Override
    public float idf(long docFreq, long numDocs) {
        // more-heavily weight terms that appear infrequently
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("HELLO I AM in idf");
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("docFreq : " + docFreq);
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("numDocs : " + numDocs);
        return (float) (Math.sqrt(numDocs/(double)(docFreq+1)) + 1.0);
    }

    @Override
    public float tf(float freq) {
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Hello I am in tf");
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("freq : " + freq);
        return super.tf(freq);
    }

  /** Implemented as <code>overlap / maxOverlap</code>. */
  @Override
  public float coord(int overlap, int maxOverlap) {
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Hello I am in coord");
    return overlap / (float)maxOverlap;
  }

  /** Implemented as <code>1/sqrt(sumOfSquaredWeights)</code>. */
  @Override
  public float queryNorm(float sumOfSquaredWeights) {
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Hello I am in querynorm (" + sumOfSquaredWeights + ")");
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Hello I am in querynorm");
    return (float)(1.0 / Math.sqrt(sumOfSquaredWeights));
  }

  /** Implemented as <code>1 / (distance + 1)</code>. */
  @Override
  public float sloppyFreq(int distance) {
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Hello I am in sloppyFreq");
    return 1.0f / (distance + 1);
  }
  
  /** The default implementation returns <code>1</code> */
  @Override
  public float scorePayload(int doc, int start, int end, BytesRef payload) {
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Hello I am in scorePayload");
    return 1;
  }

  public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Hello I am in computePayloadFactor");
      return scorePayload(doc, start, end, payload);
  }
}
