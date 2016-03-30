##Non updated yet
from cosmos import Tool, abstract_input_taskfile as inp, abstract_output_taskfile as out
from configparser import ConfigParser
import os
import sys
from wga_settings import s

class IndexBam(Tool):
    
    inputs = [inp(format='bam')]
    outputs = [out('out', format='bam'), out('out', format='bai')]
    time_req = 12*60
    mem_req = 3000

    def cmd(self,(input_bam, ), (out_bam, out_bai )):
        return "{s[samtools]} index {input_bam} {out_bai} && cp {input_bam} {out_bam}".format(s=s,**locals())