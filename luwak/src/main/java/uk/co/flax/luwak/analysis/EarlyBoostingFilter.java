package uk.co.flax.luwak.analysis;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.LinkedList;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.BoostAttribute;

/**
 * TokenFilter that removes possessives (trailing 's) from words.
 */
public final class EarlyBoostingFilter extends TokenFilter {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final BoostAttribute boostAtt = addAttribute(BoostAttribute.class);
  private final PositionIncrementAttribute piAtt = addAttribute(PositionIncrementAttribute.class);

  private int startOffset = 0;
  private float boostFactor = 100.0f;
  private int tokenCreationFactor = 10;
  private int tokenPosition = 1;
  private final LinkedList<String> extraTokens = new LinkedList<String>();
  private State savedState;

  public EarlyBoostingFilter(TokenStream input) {
    super(input);
  }

  @Override
  public boolean incrementToken() throws IOException {
    /*
    if (!extraTokens.isEmpty()) {
        //rewind
        restoreState(savedState);
        termAtt.setEmpty().append(extraTokens.remove());
        return true;
    }
    */
    if (!input.incrementToken()) {
      return false;
    }
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("EarlyBoostingFilter offsetAtt.startOffset() : " + offsetAtt.startOffset());
    
    /*
    boostAtt.setBoost(boostFactor);
    if (boostFactor > 1.0f) {
        boostFactor /= 10.0f;
    }
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("EarlyBoostingFilter termAtt : " + termAtt);
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("EarlyBoostingFilter boostAtt : " + boostAtt.getBoost());
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("input refelct : " + input.reflectAsString(true));
    */

    /*
    for (int i = 0; i < tokenCreationFactor; i++) {
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("adding " + termAtt.toString());
        extraTokens.add(termAtt.toString());
    }
    tokenCreationFactor /= 2;
    savedState = captureState();
    */

    /*
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("setting positionIncrement to " + tokenPosition);
    piAtt.setPositionIncrement(tokenPosition);
    tokenPosition++;
    */

    int tokenLength = offsetAtt.endOffset() - offsetAtt.startOffset();
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("token offset length " + tokenLength);
    if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("setting token offset to " + startOffset + ", " + (startOffset + tokenLength));
    offsetAtt.setOffset(startOffset, startOffset + tokenLength);
    startOffset += tokenLength;

    return true;
  }
}

