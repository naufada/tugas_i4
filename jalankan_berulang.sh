#!/bin/bash
# Menjalankan program berulang kali dan merangkum hasilnya
# Pemakaian: ./jalankan_berulang.sh ./race 10
PROG=$1; N=${2:-10}
printf "%-4s %-9s %-7s %-9s %-11s %-10s\n" \
  Run Terkirim Hilang Duplikat count_akhir Status
for i in $(seq 1 $N); do
  out=$($PROG)
  t=$(echo "$out" | awk -F': ' '/Data terkirim/{print $2}')
  h=$(echo "$out" | awk -F': ' '/Data hilang/{print $2}')
  d=$(echo "$out" | awk -F': ' '/Data duplikat/{print $2}')
  c=$(echo "$out" | awk -F': ' '/count akhir/{print $2}' | awk '{print $1}')
  s=$(echo "$out" | grep -q KONSISTEN && echo KONSISTEN || echo RACE)
  printf "%-4s %-9s %-7s %-9s %-11s %-10s\n" $i $t $h $d $c $s
done
