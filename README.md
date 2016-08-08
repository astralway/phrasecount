# Phrase Count

[![Build Status](https://travis-ci.org/astralway/phrasecount.svg?branch=master)](https://travis-ci.org/astralway/phrasecount)

An example application that computes phrase counts for unique documents using Apache Fluo. Each
unique document that is added causes phrase counts to be incremented. Unique documents have
reference counts based on the number of locations that point to them. When a unique document is no
longer referenced by any location, then the phrase counts will be decremented appropriately.

After phrase counts are incremented, export transactions send phrase counts to an Accumulo table.
The purpose of exporting data is to make it available for query. Percolator is not designed to
support queries, because its transactions are designed for throughput and not responsiveness.

This example uses the Collision Free Map and Export Queue from [Apache Fluo Recipes][11]. A
Collision Free Map is used to calculate phrase counts. An Export Queue is used to update the
external Accumulo table in a fault tolerant manner. Before using Fluo Recipes, this example was
quite complex. Switching to Fluo Recipes dramatically simplified this example.

## Schema

### Fluo Table Schema

This example uses the following schema for the table used by Apache Fluo.
  
Row          | Column        | Value             | Purpose
-------------|---------------|-------------------|---------------------------------------------------------------------
uri:\<uri\>  | doc:hash      | \<hash\>          | Contains the hash of the document found at the URI
doc:\<hash\> | doc:content   | \<document\>      | The contents of the document
doc:\<hash\> | doc:refCount  | \<int\>           | The number of URIs that reference this document 
doc:\<hash\> | index:check   | empty             | Setting this columns triggers the observer that indexes the document 
doc:\<hash\> | index:status  | INDEXED or empty  | Used to track the status of whether this document was indexed 

Additionally the two recipes used by the example store their data in the table
under two row prefixes.  Nothing else should be stored within these prefixes.
The collision free map used to compute phrasecounts stores data within the row
prefix `pcm:`.  The export queue stores data within the row prefix `aeq:`.

### External Table Schema

This example uses the following schema for the external Accumulo table.

Row        | Column          | Value      | Purpose
-----------|-----------------|------------|---------------------------------------------------------------------
\<phrase\> | stat:totalCount | \<count\>  | For a given phrase, the value is the total number of times that phrase occurred in all documents.
\<phrase\> | stat:docCount   | \<count\>  | For a given phrase, the values is the number of documents in which that phrase occurred.

[PhraseCountTable][14] encapsulates all of the code for interacting with this
external table.

## Code Overview

Documents are loaded into the Fluo table by [DocumentLoader][1] which is
executed by [Load][2].  [DocumentLoader][1] handles reference counting of
unique documents and may set a notification for [DocumentObserver][3].
[DocumentObserver][3] increments or decrements global phrase counts by
inserting `+1` or `-1` into a collision free map for each phrase in a document.
[PhraseMap][4] contains the code called by the collision free map recipe.  The
code in [PhraseMap][4] does two things.  First it computes the phrase counts by
summing the updates.  Second it places the newly computed phrase count on an
export queue.  [PhraseExporter][5] is called by the export queue recipe to
generate mutations to update the external Accumulo table.
    
All observers and recipes are configured by code in [Application][10].  All
observers are run by the Fluo worker processes when notifications trigger them.

## Building

After cloning this repository, build with following command. 
 
```
mvn package 
```

## Running via Maven

If you do not have Accumulo, Hadoop, Zookeeper, and Fluo setup, then you can
start an MiniFluo instance with the [mini.sh](bin/mini.sh) script.  This script
will run [Mini.java][12] using Maven.  The command will create a
`fluo.properties` file that can be used by the other commands in this section.

```bash
./bin/mini.sh /tmp/mac fluo.properties
```

After the mini command prints out `Wrote : fluo.properties` then its ready to
use.  Run `tail -f mini.log` and look for the message about writing
fluo.properties.

This command will automatically configure [PhraseExporter][5] to export phrases
to an Accumulo table named `pcExport`.

The reason `-Dexec.classpathScope=test` is set is because it adds the test
[log4j.properties][7] file to the classpath.

### Adding documents

The [load.sh](bin/load.sh) runs [Load.java][2] which scans the directory
`$TXT_DIR` looking for .txt files to add.  The scan is recursive.  

```bash
./bin/load.sh fluo.properties $TXT_DIR
```

### Printing phrases

After documents are added, [print.sh](bin/print.sh) will run [Print.java][13]
which prints out phrase counts.  Try modifying a document you added and running
the load command again, you should eventually see the phrase counts change.

```bash
./bin/print.sh fluo.properties pcExport
```

The command will print out the number of unique documents and the number
of processed documents.  If the number of processed documents is less than the
number of unique documents, then there is still work to do.  After the load
command runs, the documents will have been added or updated.  However the
phrase counts will not update until the Observer runs in the background. 

### Killing mini

Make sure to kill mini when finished testing.  The following command will kill it.

```bash
pkill -f phrasecount.cmd.Mini
```

## Deploying example

The following script can run this example on a cluster using the Fluo
distribution and serves as executable documentation for deployment.  The
previous maven commands using the exec plugin are convenient for a development
environment, using the following scripts shows how things would work in a
production environment.

  * [run.sh] (bin/run.sh) : Runs this example with YARN using the Fluo tar
    distribution.  Running in this way requires setting up Hadoop, Zookeeper,
    and Accumulo instances separately.  The [Uno][8] and [Muchos][9]
    projects were created to ease setting up these external dependencies.

## Generating data

Need some data? Use `elinks` to generate text files from web pages.

```
mkdir data
elinks -dump 1 -no-numbering -no-references http://accumulo.apache.org > data/accumulo.txt
elinks -dump 1 -no-numbering -no-references http://hadoop.apache.org > data/hadoop.txt
elinks -dump 1 -no-numbering -no-references http://zookeeper.apache.org > data/zookeeper.txt
```

[1]: src/main/java/phrasecount/DocumentLoader.java
[2]: src/main/java/phrasecount/cmd/Load.java
[3]: src/main/java/phrasecount/DocumentObserver.java
[4]: src/main/java/phrasecount/PhraseMap.java
[5]: src/main/java/phrasecount/PhraseExporter.java
[7]: src/test/resources/log4j.properties
[8]: https://github.com/astralway/uno
[9]: https://github.com/astralway/muchos
[10]: src/main/java/phrasecount/Application.java
[11]: https://github.com/apache/fluo-recipes
[12]: src/main/java/phrasecount/cmd/Mini.java
[13]: src/main/java/phrasecount/cmd/Print.java
[14]: src/main/java/phrasecount/query/PhraseCountTable.java
