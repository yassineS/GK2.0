##Non updated yet
from cosmos import Tool, abstract_input_taskfile as inp, abstract_output_taskfile as out
from configparser import ConfigParser
import os
import sys
opj = os.path.join
parent = os.path.normpath(os.path.join(os.path.realpath(__file__), "..", ".."))
config = ConfigParser()
config.read(os.path.join(parent, 'settings.conf'))
s = config['wga_settings']


class BamToBWA(Tool):
    name = "BWA Alignement"
    cpu_req = 4
    mem_req = 12 * 1024
    time_req = 2 * 60
    inputs = [inp(format='bam', n='>0')]
    outputs = [out('bam', format='bam'), out('bam', format='bai')]

    #def cmd(self, i, s, p):
    def cmd(self, (i, ), s):
        # removed -m MEM option in samtools sort

        # Chromosome and RG split

        return r"""
                rg=$({s[samtools]} view -H {i[bam][0]} | grep -w {p[rgId]} | uniq | sed 's/\t/\\t/g') && echo "RG= $rg";
                {s[samtools]} view -f 2 -u -r {p[rgId]} {i[bam][0]} {p[prevSn]} > tmpIn.ubam;

	            cp {s[empty_sam]} empty.sam && echo -e $rg >> empty.sam;
                {s[samtools]} view -u {i[bam][0]} "empty_region" > empty.ubam 2> /dev/null;

                sizeEmpty="$(du -b empty.ubam | cut -f 1)"; printf "Empty bam file size = %-6d\n" "$sizeEmpty";
                sizeTmpIn="$(du -b tmpIn.ubam | cut -f 1)"; printf "Input bam file size = %-6d\n" "$sizeTmpIn";

                [[ "$sizeTmpIn" -gt "$sizeEmpty" ]] &&
                {s[samtools]} sort -n -o -l 0 -@ {self.cpu_req} tmpIn.ubam _shuf |
                {s[bamUtil]} bam2FastQ --in -.ubam --readname --noeof --firstOut /dev/stdout --merge --unpairedout un.fq 2> /dev/null |
                {s[bwa]} mem -p -M -t {self.cpu_req} -R "$rg" {s[reference_fasta]} - |
                {s[samtools]} view -Shu - |
                {s[samtools]} sort    -o -l 0 -@ {self.cpu_req} - _sort > out.bam;

                # If there's no out.bam available, put an empty bam as output;
                [[ ! -a out.bam ]] && ({s[samtools]} view -Sb empty.sam > out.bam 2> /dev/null) || true;

	            {s[samtools]} index out.bam out.bai;

                """.format(input=''.join(map(str, (i,))), s=s,**locals())


class BamToBWAchr(Tool):
    name = "BWA Alignement"
    cpu_req = 4
    mem_req = 12 * 1024
    time_req = 2 * 60
    inputs = [inp(format='bam', n='>0')]
    outputs = [out('bam', format='bam'), out('bam', format='bai')]


    def cmd(self, (i, ), s):
        # removed -m MEM option in samtools sort

        # chromosome_only_split: Using first readgroup id

        return r"""
                rg=$({s[samtools]} view -H {i[bam][0]} | grep -w "@RG" | head -n 1 | sed 's/\t/\\t/g') && echo "RG= $rg";
                {s[samtools]} view -f 2 -u              {i[bam][0]} {p[prevSn]} > tmpIn.ubam;

	            cp {s[empty_sam]} empty.sam && echo -e $rg >> empty.sam;
                {s[samtools]} view -u {i[bam][0]} "empty_region" > empty.ubam 2> /dev/null;

                sizeEmpty="$(du -b empty.ubam | cut -f 1)"; printf "Empty bam file size = %-6d\n" "$sizeEmpty";
                sizeTmpIn="$(du -b tmpIn.ubam | cut -f 1)"; printf "Input bam file size = %-6d\n" "$sizeTmpIn";

                [[ "$sizeTmpIn" -gt "$sizeEmpty" ]] &&
                {s[samtools]} sort -n -o -l 0 -@ {self.cpu_req} tmpIn.ubam _shuf |
                {s[bamUtil]} bam2FastQ --in -.ubam --readname --noeof --firstOut /dev/stdout --merge --unpairedout un.fq 2> /dev/null |
                {s[bwa]} mem -p -M -t {self.cpu_req} -R "$rg" {s[reference_fasta]} - |
                {s[samtools]} view -Shu - |
                {s[samtools]} sort    -o -l 0 -@ {self.cpu_req} - _sort > out.bam;

                # If there's no out.bam available, put an empty bam as output;
                [[ ! -a out.bam ]] && ({s[samtools]} view -Sb empty.sam > out.bam 2> /dev/null) || true;

	            {s[samtools]} index out.bam out.bai;

                """.format(input=''.join(map(str, (i,))), s=s,**locals())



                