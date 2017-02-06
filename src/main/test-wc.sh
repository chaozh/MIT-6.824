#!/bin/bash
go run wc.go master sequential pg-*.txt
sort -n -k2 mrtmp.wcseq | tail -10 | diff - mr-testout.txt > diff.out
if [ -s diff.out ]
then
echo "Failed test. Output should be as in mr-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat diff.out
else
  echo "Passed test" > /dev/stderr
fi

