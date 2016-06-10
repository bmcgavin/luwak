package uk.co.flax.luwak.similarities;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.DefaultSimilarity;

public class StartBoostSimilarity extends DefaultSimilarity {

    @Override
    public float lengthNorm(FieldInvertState state) {
        // simply return the field's configured boost value
        // instead of also factoring in the field's length
        System.out.println("HELLO I AM IN lengthNorm");
        System.out.println("getAttributeSource : " + state.getAttributeSource());
        System.out.println("getBoost : " + state.getBoost());
        System.out.println("getLength : " + state.getLength());
        System.out.println("getMaxTermFrequency : " + state.getMaxTermFrequency());
        System.out.println("getName : " + state.getName());
        System.out.println("getNumOverlap : " + state.getNumOverlap());
        System.out.println("getOffset : " + state.getOffset());
        System.out.println("getPosition : " + state.getPosition());
        System.out.println("getUniqueTermCount : " + state.getUniqueTermCount());
        return state.getBoost();
    }

    @Override
    public float idf(long docFreq, long numDocs) {
        // more-heavily weight terms that appear infrequently
        System.out.println("HELLO I AM in idf");
        System.out.println("docFreq : " + docFreq);
        System.out.println("numDocs : " + numDocs);
        return (float) (Math.sqrt(numDocs/(double)(docFreq+1)) + 1.0);
    }

    @Override
    public float tf(float freq) {
        System.out.println("Hello I am in tf");
        System.out.println("freq : " + freq);
        return super.tf(freq);
    }
}
