#!/bin/bash

mvn -Dmaven.test.skip=true package && cp /Users/rjones/luwak/luwak/target/luwak-1.3.0.jar ~/.m2/repository/com/github/flaxsearch/luwak/1.3.0/