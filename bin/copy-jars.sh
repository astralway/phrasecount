#!/bin/bash

#This script will copy the phrase count jar and its dependencies to the Fluo
#application lib dir


if [ "$#" -ne 2 ]; then
  echo "Usage : $0 <FLUO HOME> <PHRASECOUNT_HOME>"
  exit 
fi

FLUO_HOME=$1
PC_HOME=$2

PC_JAR=$PC_HOME/target/phrasecount-0.0.1-SNAPSHOT.jar

#build and copy phrasecount jar
(cd $PC_HOME; mvn package -DskipTests)

FLUO_APP_LIB=$FLUO_HOME/apps/phrasecount/lib/

cp $PC_JAR $FLUO_APP_LIB
(cd $PC_HOME; mvn dependency:copy-dependencies -DoutputDirectory=$FLUO_APP_LIB)

