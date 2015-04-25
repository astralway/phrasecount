#!/bin/bash

if [ "$#" -ne 3 ]; then
  echo "Usage : $0 <FLUO HOME> <PHRASECOUNT_HOME> <TXT FILES DIR>"
  exit 
fi

FLUO_HOME=$1
PC_HOME=$2
#set the following to a directory containg text files
TXT_DIR=$3

#derived variables
APP_PROPS=$FLUO_HOME/apps/phrasecount/conf/fluo.properties
CLIENT_PROPS=$FLUO_HOME/apps/phrasecount/mini/client.properties
PC_JAR=$PC_HOME/target/phrasecount-0.0.1-SNAPSHOT.jar
#TODO should not be pulling log4j jars from FLUO_HOME/lib
export CLASSPATH=$PC_JAR:`$FLUO_HOME/bin/mini-fluo classpath -a -z -H`:$FLUO_HOME/lib/log4j/*

if [ ! -f $FLUO_HOME/conf/fluo.properties ]; then
  #only do this once after untarring
  cp $FLUO_HOME/conf/examples/* $FLUO_HOME/conf
fi

#create new application dir
$FLUO_HOME/bin/mini-fluo new phrasecount

#build and copy phrasecount jar
(cd $PC_HOME; mvn package -DskipTests)
cp $PC_JAR $FLUO_HOME/apps/phrasecount/lib/

#configure Fluo
#TODO could issue a sed command to delete existing observer config if want to support repeated runs of in a script
echo 'io.fluo.observer.0=phrasecount.PhraseCounter' >> $APP_PROPS
echo 'io.fluo.observer.1=phrasecount.PhraseExporter,sink=accumulo,instance=${io.fluo.client.accumulo.instance},zookeepers=${io.fluo.client.accumulo.zookeepers},user=${io.fluo.client.accumulo.user},password=${io.fluo.client.accumulo.password},table=pcExport' >> $APP_PROPS
echo 'io.fluo.observer.weak.0=phrasecount.HCCounter' >> $APP_PROPS

#start mini
$FLUO_HOME/bin/mini-fluo start phrasecount

#Load data
java phrasecount.cmd.Load $CLIENT_PROPS $TXT_DIR

#wait for all notifications to be processed.
$FLUO_HOME/bin/mini-fluo wait phrasecount

#print phrase counts
java phrasecount.cmd.Print $CLIENT_PROPS 

#compare phrasecounts in Fluo table and export table 
java phrasecount.cmd.Compare $CLIENT_PROPS phrasecount pcExport

#cleanup, may want to comment out
$FLUO_HOME/bin/mini-fluo kill phrasecount
rm -rf $FLUO_HOME/apps/phrasecount

