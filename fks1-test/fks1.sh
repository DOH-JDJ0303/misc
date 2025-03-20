#!/bin/bash
PREFIX=$1
R1=$2
R2=$3
REF_FASTA=$4
REF_GFF=$5

if [ ! -f $NAME.$REFBASE.bam ]
then
    bwa index $REF_FASTA
    bwa mem $REF_FASTA $R1 $R2 | samtools view -b -F 4 - | samtools sort - > $PREFIX.bam
fi
samtools mpileup -aa -A -d 0 -B -Q 0 --reference CAB11_002014T0.fna $PREFIX.bam | ivar variants -p $PREFIX -r $REF_FASTA -m 10 -g $REF_GFF -t 0.75
cat $PREFIX.tsv | \
    awk '{ \
    if ( NR == 1 ) print $0, "HOTSPOT"  ; \
    else if ( $2 > 1901 && $2 < 1929 ) print $0, "hs1" ; \
    else if ( $2 > 4046 && $2 < 4071 ) print $0,"hs2" ; \
    else if ( $2 > 2069 && $2 < 2073 ) print $0, "hs3" ; \
    else print $0, "" }' | \
    awk '{\
    if ( NR == 1 ) print $0, "MUTATION" ; \
    else if ( $21 != "" ) print $0, $17$20$19 ; \
    else print $0, "" }' > $PREFIX.anno.tsv

cat $PREFIX.anno.tsv | awk '$22 != "" && NR != 1 {print $22}' > $PREFIX.mutations.txt
