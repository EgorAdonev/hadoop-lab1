#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify output file!'
    exit 1
fi

# prefix doesn't mean something - it can be repetable
PREFIX='Apr 17 2022 04:06:01'
a=' localhost '
b='kernel: '
c=' gvfsd-admin uses 32-bit capabilities (legacy)'
colon=':'
rm -rf input
mkdir input

#for str in ${myArray[@]}; do
#  echo $str >> input/$1.2
#done
for i in {1..100}
   do
     #severity=(1 + $RANDOM % 7)
     RESULT="${PREFIX}${a}${b}$((1 + $RANDOM % 5))${colon}${c}"
     echo $RESULT >> input/$1.2
   done

for i in {1..200}
   do
     #severity=(1 + $RANDOM % 7)
     RESULT="${PREFIX}${a}${b}$((1 + $RANDOM % 7))${colon}${c}"
     echo $RESULT >> input/$1.1
   done

#rm -rf input2
#mkdir input2
#
#for i in {1..200}
#   do
#     OUTPUT=$(cat /var/log/messages)
#     echo ${OUTPUT} >> input2/$1.1
#   done
