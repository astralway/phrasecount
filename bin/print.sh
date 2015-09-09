#!/bin/bash

mvn exec:java -Dexec.mainClass=phrasecount.cmd.Print -Dexec.args="${*:1}" -Dexec.classpathScope=test

