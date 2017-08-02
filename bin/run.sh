#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

if [[ "$OSTYPE" == "darwin"* ]]; then
  sed_cmd="sed -i .bak"
else
  sed_cmd="sed -i"
fi

# stop if any command fails
set -e

if [ "$#" -ne 1 ]; then
  echo "Usage : $0 <TXT FILES DIR>"
  exit 
fi

# set the following to a directory containing text files
txt_dir=$1
if [ ! -d "$txt_dir" ]; then
  echo "Document directory $txt_dir does not exist" 
  exit 1
fi

#ensure $FLUO_HOME is set
if [ -z "$FLUO_HOME" ]; then
  echo "$FLUO_HOME must be set!"
  exit 1
fi

fluo=$FLUO_HOME/bin/fluo
app=phrasecount
app_props=$FLUO_HOME/conf/${app}.properties
conn_props=$FLUO_HOME/conf/fluo-conn.properties

if [ -f "$app_props" ]; then
  echo "Restarting '$app' application.  Errors may be printed if it's not running..."
  $fluo stop "$app" || true
  rm "$app_props"
fi

cp "$FLUO_HOME/conf/fluo-app.properties" "$app_props"

app_lib=$PC_HOME/target/lib
mkdir -p "$app_lib"
(cd "$PC_HOME"; mvn package -DskipTests)
cp "$PC_HOME/target/phrasecount-0.0.1-SNAPSHOT.jar" "$app_lib"
(cd "$PC_HOME"; mvn dependency:copy-dependencies -DoutputDirectory="$app_lib")

$sed_cmd "s#^.*fluo.observer.init.dir=[^ ]*#fluo.observer.init.dir=${app_lib}#" "$app_props"

# Create export table and output Fluo configuration
java -cp "$app_lib/*:$("$fluo" classpath)" phrasecount.cmd.Setup "$conn_props" "$app_props" pcExport >> "$app_props"

$fluo init $app "$app_props" -f
$fluo exec $app org.apache.fluo.recipes.accumulo.cmds.OptimizeTable
$fluo oracle $app
$fluo worker $app

# Load data
$fluo exec $app phrasecount.cmd.Load "$conn_props" $app "$txt_dir"

# Wait for all notifications to be processed.
$fluo wait $app

# Print phrase counts
$fluo exec $app phrasecount.cmd.Print "$conn_props" $app pcExport

$fluo stop $app
