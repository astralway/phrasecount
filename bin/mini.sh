#!/bin/bash

mvn exec:java -Dexec.mainClass=phrasecount.cmd.Mini -Dexec.args="${*:1}" -Dexec.classpathScope=test &>mini.log &
echo "Started Mini in background.  Writing output to mini.log."
