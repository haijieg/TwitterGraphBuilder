#!/bin/sh
path=$1 
echo "Renaming files in $path"
for file in `ls $path`
do
  find=`echo $file | grep "\:"`
  if [ $find ]
  then
    echo $file '->' `echo $file | sed 's/\://'` 
    cd $path
    mv $file `echo $file | sed 's/\://'`
  fi
done
