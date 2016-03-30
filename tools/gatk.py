##Non updated yet
from cosmos import Tool, abstract_input_taskfile as inp, abstract_output_taskfile as out
from configparser import ConfigParser
import os
import sys
from wga_settings import s


# Indel Realigner

class IndelRealigner(Tool):
    name = "Indel Realigner"
    cpu_req = 4
    mem_req = 12 * 1024
    time_req = 4 * 60
    inputs = [inp(format='bam', n='>0')]
    outputs = [out('out', format='bam'), out('out', format='bai')]

    def cmd(self, i, s, p):
        cmd_main = r"""
            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T RealignerTargetCreator
            -R {s[reference_fasta]}
            -o $tmpDir/{p[chrom]}.intervals
            --known {s[1kindel_vcf]}
            --known {s[mills_vcf]}
            --num_threads {self.cpu_req}
            -L {p[chrom]} {s[gatk_realigntarget]}
            {inputs};

            printf "\n%s RealignerTargetCreator ended.\n" "{s[date]}" | tee -a /dev/stderr;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T IndelRealigner
            -R {s[reference_fasta]}
            -o $tmpDir/out.bam
            -targetIntervals $tmpDir/{p[chrom]}.intervals
            -known {s[1kindel_vcf]}
            -known {s[mills_vcf]}
            -model USE_READS
            -compress 0
            -L {p[chrom]} {s[gatk_indelrealign]}
            {inputs};
        """
        return (cmd_init + cmd_main + cmd_out), \
               dict(
                   inputs=_list2input(i['bam'], "-I "),
                   s=s,
                   **locals()
               )


class MarkDuplicates(Tool):
    name = "MarkDuplicates"
    cpu_req = 2
    mem_req = 4 * 1024
    time_req = 2 * 60
    inputs = [inp(format='bam', n='>0')]
    outputs = [out('out', format='bam'), out('out', format='bai'), out('out', format='metrics')]

    def cmd(self, i, s, p):
        cmd_main = r"""

            {s[java]} -Xmx{self.mem_req}M -jar {s[picard_dir]}/MarkDuplicates.jar
            TMP_DIR=$tmpDir
            OUTPUT=$tmpDir/out.bam
            METRICS_FILE=$tmpDir/out.metrics
            ASSUME_SORTED=True
            CREATE_INDEX=True
            COMPRESSION_LEVEL=0
            MAX_RECORDS_IN_RAM=1000000
            VALIDATION_STRINGENCY=SILENT
            VERBOSITY=INFO
            {inputs};

            mv -f $tmpDir/out.metrics $OUT.metrics;
        """
        return (cmd_init + cmd_main + cmd_out), \
               dict(
                   inputs=_list2input(i['bam'], "INPUT="),
                   s=s,
                   **locals()
               )


class BQSR(Tool):
    name = "Base Quality Score Recalibration"
    cpu_req = 4
    mem_req = 12 * 1024
    time_req = 4 * 60
    inputs = [inp(format='bam', n='>0')]
    outputs = [out('out', format='bam'), out('out', format='bai')]  # Need to check the persistence option

    # no -nt, -nct = 4
    def cmd(self, i, s, p):
        cmd_main = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T BaseRecalibrator
            -R {s[reference_fasta]}
            -o $tmpDir/{p[chrom]}.grp
            -knownSites {s[dbsnp_vcf]}
            -knownSites {s[1komni_vcf]}
            -knownSites {s[1kindel_vcf]}
            -knownSites {s[mills_vcf]}
            -nct {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            printf "\n%s BaseRecalibrator ended\n" "{s[date]}" | tee -a /dev/stderr;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T PrintReads
            -R {s[reference_fasta]}
            -o $tmpDir/out.bam
            -compress 0
            -BQSR $tmpDir/{p[chrom]}.grp
            -nct {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            """
        return (cmd_init + cmd_main + cmd_out), \
               dict(
                   inputs=_list2input(i['bam'], "-I "),
                   s=s,
                   **locals()
               )


# Mean to be used per sample
class HaplotypeCaller(Tool):
    name = "Haplotype Caller"
    cpu_req = 4
    mem_req = 16 * 1024
    time_req = 12 * 60
    inputs = [inp(format='bam', n='>0')]
    outputs = [out('out', format='vcf'), out('out', format='vcf.idx')]

    # -nct available
    def cmd(self, i, s, p):
        cmd_main = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T HaplotypeCaller
            -R {s[reference_fasta]}
            -D {s[dbsnp_vcf]}
            -o $tmpDir/out.vcf
            -pairHMM VECTOR_LOGLESS_CACHING
            -L {p[chrom]}
            -nct 1
            --emitRefConfidence GVCF --variant_index_type LINEAR --variant_index_parameter 128000
            -A DepthPerAlleleBySample
            -stand_call_conf 30
            -stand_emit_conf 10
            {inputs};

            """
        return (cmd_init + cmd_main + cmd_out_vcf), \
               dict(
                   inputs=_list2input(i['vcf'], "-I "),
                   s=s,
                   **locals()
               )

# Joint Genotyping
class GenotypeGVCFs(Tool):
    name = "Genotype GVCFs"
    cpu_req = 4
    mem_req = 12 * 1024
    time_req = 12 * 60
    inputs = [inp(format='vcf', n='>0')]
    outputs = [out('out', format='vcf'), out('out', format='vcf.idx')]

    # -nt available
    def cmd(self, i, s, p):
        cmd_main = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T GenotypeGVCFs
            -R {s[reference_fasta]}
            -D {s[dbsnp_vcf]}
            -o $tmpDir/out.vcf
            -L {p[chrom]}
            -nt {self.cpu_req}
            -A Coverage
            -A GCContent
            -A HaplotypeScore
            -A MappingQualityRankSumTest
            -A InbreedingCoeff -A FisherStrand -A QualByDepth -A ChromosomeCounts
            {inputs};

            """
        return (cmd_init + cmd_main + cmd_out_vcf), \
               dict(
                   inputs=_list2input(i['vcf'], "-V "),
                   s=s,
                   **locals()
               )

class VQSR(Tool):
    """
    VQSR
    Note that HaplotypeScore is no longer applicable to indels
    see http://gatkforums.broadinstitute.org/discussion/2463/unified-genotyper-no-haplotype-score-annotated-for-indels

    """
    name = "Variant Quality Score Recalibration"
    cpu_req = 4
    mem_req = 12 * 1024
    time_req = 12 * 60
    inputs = [inp(format='vcf', n='>0')]
    outputs = [out('out', format='vcf'), out('out', format='vcf.idx'), out('out', format='R')]


    # -nt available, -nct not available
    def cmd(self, i, s, p):
        """
        Check gatk forum: http://gatkforums.broadinstitute.org/discussion/1259/what-vqsr-training-sets-arguments-should-i-use-for-my-specific-project
        --maxGaussians         8 (default), set    1 for small-scale test
        --minNumBadVariants 1000 (default), set 3000 for small-scale test
        """
        cmd_VQSR = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T VariantRecalibrator
            -R {s[reference_fasta]}
            -recalFile    $tmpDir/out.recal
            -tranchesFile $tmpDir/out.tranches
            -rscriptFile  $tmpDir/out.R
            -nt {self.cpu_req}
            -an MQRankSum -an ReadPosRankSum -an DP -an FS -an QD
            -mode {p[glm]}
            -L {p[chrom]}
            --maxGaussians 1
            --minNumBadVariants 3000
            {inputs}
            """
        cmd_SNP = r"""
            -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {s[hapmap_vcf]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0  {s[dbsnp_vcf]}
            -resource:omni,known=false,training=true,truth=true,prior=12.0   {s[1komni_vcf]}
            -resource:1000G,known=false,training=true,truth=false,prior=10.0 {s[1ksnp_vcf]};
            """
        cmd_INDEL = r"""
            -resource:mills,known=false,training=true,truth=true,prior=12.0 {s[mills_vcf]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0 {s[dbsnp_vcf]};
            """
        cmd_apply_VQSR = r"""

            printf "\n%s\n" "{s[date]}" | tee -a /dev/stderr;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T ApplyRecalibration
            -R {s[reference_fasta]}
            -recalFile    $tmpDir/out.recal
            -tranchesFile $tmpDir/out.tranches
            -o            $tmpDir/out.vcf
            --ts_filter_level 99.9
            -mode {p[glm]}
            -nt {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            mv -f $tmpDir/out.R $OUT.R;

            """
        if p['glm'] == 'SNP':
            cmd_rc = cmd_SNP
        else:
            cmd_rc = cmd_INDEL

        if p['skip_VQSR']:
            return " cp {i[vcf][0]} $OUT.vcf; cp {i[vcf][0]}.idx $OUT.vcf.idx; touch $OUT.R"
        else:
            return (cmd_init + cmd_VQSR + cmd_rc + cmd_apply_VQSR + cmd_out_vcf), \
               dict(
                   inputs=_list2input(i['vcf'], "-input "),
                   s=s,
                   **locals()
               )

class CombineVariants(Tool):
    name = "Combine Variants"
    cpu_req = 4  # max CPU here
    mem_req = 12 * 1024
    time_req = 2 * 60
    inputs = [inp(format='vcf', n='>0')]
    outputs = [out('out', format='vcf'), out('out', format='vcf.idx')]

    # -nt available, -nct not available
    # Too many -nt (20?) will cause write error
    def cmd(self, i, s, p):
        """
        :param genotypemergeoptions: select from the following:
            UNIQUIFY       - Make all sample genotypes unique by file. Each sample shared across RODs gets named sample.ROD.
            PRIORITIZE     - Take genotypes in priority order (see the priority argument).
            UNSORTED       - Take the genotypes in any order.
            REQUIRE_UNIQUE - Require that all samples/genotypes be unique between all inputs.
        """
        cmd_main = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T CombineVariants
            -R {s[reference_fasta]}
            -o $tmpDir/out.vcf
            -genotypeMergeOptions UNSORTED
            -nt {self.cpu_req}
            {inputs};

        """
        return (cmd_init + cmd_main + cmd_out_vcf), \
               dict(
                   inputs=_list2input(i['vcf'], "-V "),
                   s=s,
                   **locals()
               )
