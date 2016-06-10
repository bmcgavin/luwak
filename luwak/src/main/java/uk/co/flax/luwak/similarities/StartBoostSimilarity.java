package uk.co.flax.luwak.similarities;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.DefaultSimilarity;

public class StartBoostSimilarity extends DefaultSimilarity {

    @Override
    public float lengthNorm(FieldInvertState state) {
        // simply return the field's configured boost value
        // instead of also factoring in the field's length
        //DEBUG System.out.println("HELLO I AM IN lengthNorm");
        //DEBUG System.out.println("getAttributeSource : " + state.getAttributeSource());
        //DEBUG System.out.println("getBoost : " + state.getBoost());
        //DEBUG System.out.println("getLength : " + state.getLength());
        //DEBUG System.out.println("getMaxTermFrequency : " + state.getMaxTermFrequency());
        //DEBUG System.out.println("getName : " + state.getName());
        //DEBUG System.out.println("getNumOverlap : " + state.getNumOverlap());
        //DEBUG System.out.println("getOffset : " + state.getOffset());
        //DEBUG System.out.println("getPosition : " + state.getPosition());
        //DEBUG System.out.println("getUniqueTermCount : " + state.getUniqueTermCount());
        return state.getBoost();
    }

    @Override
    public float idf(long docFreq, long numDocs) {
        // more-heavily weight terms that appear infrequently
        //DEBUG System.out.println("HELLO I AM in idf");
        //DEBUG System.out.println("docFreq : " + docFreq);
        //DEBUG System.out.println("numDocs : " + numDocs);
        return (float) (Math.sqrt(numDocs/(double)(docFreq+1)) + 1.0);
    }

    @Override
    public float tf(float freq) {
        //DEBUG System.out.println("Hello I am in tf");
        //DEBUG System.out.println("freq : " + freq);
        return super.tf(freq);
    }
}
