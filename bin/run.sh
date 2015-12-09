#!/bin/bash

# Run script used by fluo-dev & fluo-deploy

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

if [ "$#" -ne 1 ]; then
  echo "A directory containing text documents must be specified"
  exit 1
fi

TXT_DIR=$1
if [ ! -d $TXT_DIR ]; then
  echo "Document directory $TXT_DIR does not exist" 
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

# Build phrasecount jar
(cd $PC_HOME; mvn package -DskipTests)

if [ ! -d $FLUO_HOME/apps/$FLUO_APP_NAME ]; then
  $FLUO_HOME/bin/fluo new $FLUO_APP_NAME
else
  echo "Restarting '$FLUO_APP_NAME' application.  Errors may be printed if it's not running..."
  $FLUO_HOME/bin/fluo kill $FLUO_APP_NAME || true
  rm -rf $FLUO_HOME/apps/$FLUO_APP_NAME
  $FLUO_HOME/bin/fluo new $FLUO_APP_NAME
fi

# Copy phrasecount jar
cp $PC_JAR $FLUO_HOME/apps/phrasecount/lib/

if [[ "$OSTYPE" == "darwin"* ]]; then
  export SED="sed -i .bak"
else
  export SED="sed -i"
fi

# Configure Fluo
$SED '/io.fluo.observer/d' $APP_PROPS
#need to uncomment zookeeper line, if its commented for interpolation to work
$SED 's/#io.fluo.client.accumulo.zookeepers/io.fluo.client.accumulo.zookeepers/' $APP_PROPS
echo 'io.fluo.observer.0=phrasecount.PhraseCounter' >> $APP_PROPS
echo 'io.fluo.observer.1=phrasecount.PhraseExporter,sink=accumulo,instance=${io.fluo.client.accumulo.instance},zookeepers=${io.fluo.client.accumulo.zookeepers},user=${io.fluo.client.accumulo.user},password=${io.fluo.client.accumulo.password},table=pcExport' >> $APP_PROPS
echo 'io.fluo.observer.weak.0=phrasecount.HCCounter' >> $APP_PROPS

# Initialize and start Fluo
$FLUO_HOME/bin/fluo init $FLUO_APP_NAME -f
$FLUO_HOME/bin/fluo start $FLUO_APP_NAME

# Load data
java phrasecount.cmd.Load $APP_PROPS $TXT_DIR

# Wait for all notifications to be processed.
$FLUO_HOME/bin/fluo wait phrasecount

# Print phrase counts
java phrasecount.cmd.Print $APP_PROPS 

# Compare phrasecounts in Fluo table and export table 
java phrasecount.cmd.Compare $APP_PROPS phrasecount pcExport
