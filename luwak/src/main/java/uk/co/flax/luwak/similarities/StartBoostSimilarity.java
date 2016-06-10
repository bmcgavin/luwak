package uk.co.flax.luwak.similarities;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.DefaultSimilarity;

public class StartBoostSimilarity extends DefaultSimilarity {

    @Override
    public float lengthNorm(FieldInvertState state) {
        // simply return the field's configured boost value
        // instead of also factoring in the field's length
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("HELLO I AM IN lengthNorm");
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("getAttributeSource : " + state.getAttributeSource());
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
}
