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

# Load data
java phrasecount.cmd.Load $APP_PROPS $TXT_DIR

# Wait for all notifications to be processed.
$FLUO_HOME/bin/fluo wait phrasecount

# Print phrase counts
java phrasecount.cmd.Print $APP_PROPS 

# Compare phrasecounts in Fluo table and export table 
java phrasecount.cmd.Compare $APP_PROPS phrasecount pcExport
