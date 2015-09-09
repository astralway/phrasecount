#!/bin/bash

mvn exec:java -Dexec.mainClass=phrasecount.cmd.Load -Dexec.args="${*:1}" -Dexec.classpathScope=test
