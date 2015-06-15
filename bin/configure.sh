#!/bin/bash

# Configure script used by fluo-dev & fluo-deploy

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

if [ "$#" -ne 1 ]; then
  echo "ERROR - Expected command-line argument with path to directory containing text documents"
  exit 1
fi

TXT_DIR=$1
if [ ! -d "$TXT_DIR" ]; then
  echo "ERROR - Document directory $TXT_DIR does not exist" 
  exit 1
fi

if [ -z "$FLUO_HOME" ]; then
  echo "$FLUO_HOME must be set!"
  exit 1
fi

# Derived variables
APP_PROPS=$FLUO_HOME/apps/$FLUO_APP_NAME/conf/fluo.properties
PC_JAR=$PC_HOME/target/phrasecount-0.0.1-SNAPSHOT.jar
#TODO should not be pulling log4j jars from FLUO_HOME/lib
export CLASSPATH=$PC_JAR:`$FLUO_HOME/bin/fluo classpath -a -z -H`:$FLUO_HOME/lib/log4j/*

# Build and copy phrasecount jar
(cd $PC_HOME; mvn package -DskipTests)
cp $PC_JAR $FLUO_HOME/apps/phrasecount/lib/

# Configure Fluo
sed -i '/io.fluo.observer/d' $APP_PROPS
#need to uncomment zookeeper line, if its commented for interpolation to work
sed -i 's/#io.fluo.client.accumulo.zookeepers/io.fluo.client.accumulo.zookeepers/' $APP_PROPS
echo 'io.fluo.observer.0=phrasecount.PhraseCounter' >> $APP_PROPS
echo 'io.fluo.observer.1=phrasecount.PhraseExporter,sink=accumulo,instance=${io.fluo.client.accumulo.instance},zookeepers=${io.fluo.client.accumulo.zookeepers},user=${io.fluo.client.accumulo.user},password=${io.fluo.client.accumulo.password},table=pcExport' >> $APP_PROPS
echo 'io.fluo.observer.weak.0=phrasecount.HCCounter' >> $APP_PROPS
