import os
import pysam

from cosmos import Execution, Cosmos, rel, Recipe, Input
import tools
from tools import s as genomekey_settings

def _getHeaderInfo(input_bam):
    if   input_bam[-3:] == 'bam':
        header = pysam.Samfile(input_bam,'rb', check_sq = False).header
    elif input_bam[-3:] == 'sam':
        header = pysam.Samfile(input_bam,'r' , check_sq = False).header
    else:
        raise TypeError, 'input file is not a bam or sam'

    return {'rg': [ [tags['ID'], tags['SM'], tags.get('LB','noLBinfo'), tags.get('PL','noPLinfo') ] for tags in header['RG']],
            'sq': [ [tags['SN']                                                                   ] for tags in header['SQ']]
           }

def _getSeqName(header):
    """
    Return sequence names (@SQ SN in header)
    """
    seqNameList = []
    unMapped=''
    for sn in header['sq']:
        if (sn[0].startswith('GL')) or (sn[0].startswith('chrUn')):
            unMapped += " %s" % sn[0]
        else:
            seqNameList.append(sn[0])  # first column is seqName

    if unMapped != '':
        seqNameList.append(unMapped)

    return seqNameList


def genomekey_main(bams, test_bam=False, chromosome_only_split=False):
    
    print kwargs
    bams     = kwargs['input_bams']
    test_bam = kwargs['test_bam']

    if (test_bam ==''):
        test_bam = False

    chromosome_only_split = kwargs['chromosome_only_split']

    if (chromosome_only_split == ''):
        chromosome_only_split = False

    # split_ tuples
    #chrom  = ('chrom', range(1,23) + ['X', 'Y', 'MT'])
    chrom  = ('chrom', range(1,23))

    glm = ('glm', ['SNP', 'INDEL'])

    #dbnames = ('dbname', ['dbSNP135','CytoBand','Target_Scan','mirBase','Self_Chain','Repeat_Masker','TFBS','Segmental_Duplications','SIFT','COSMIC',
    #                      'PolyPhen2','Mutation_Taster','GERP','PhyloP','LRT','Mce46way','Complete_Genomics_69','The_1000g_Febuary_all','The_1000g_April_all',
    #                      'NHLBI_Exome_Project_euro','NHLBI_Exome_Project_aa','NHLBI_Exome_Project_all','ENCODE_DNaseI_Hypersensitivity','ENCODE_Transcription_Factor',
    #                      'UCSC_Gene','Refseq_Gene','Ensembl_Gene','CCDS_Gene','HGMD_INDEL','HGMD_SNP','GWAS_Catalog'])

    for b in bams:
        header = _getHeaderInfo(b)
        sn     = _getSeqName(header)

        rgid = [ h[0] for h in header['rg']]

        # restrict output for testing
        if test_bam:
            sn    = ['chr1']
            chrom = ('chrom',[1])
            glm   = ('glm',['SNP'])
            skip_VQSR = ('skip_VQSR', [True])

        else:
            skip_VQSR = ('skip_VQSR', [False])

        # if seqName is empty, then let's assume that the input is unaligned bam
        # use everything before extension as part of tag

        sample_name = os.path.splitext(os.path.basename(b))[0]


        recipe= Recipe()
        # Pipeline inputs
        add  = recipe.add_source([Input(name=b, format='bam', tags=dict(bam=[sample_name], rgId=[rgid], chrom=[chrom])])

        if chromosome_only_split:
            # Stop splitting by rgId
            align           = recipe.add_stage(bwa.BamToBWAchr, add, rel.One2many(['sample_name', 'chrom']), tags=dict(bam=[sample_name], chrom=[chrom], chromosome_only_split=True))
        else:

            align = recipe.add_stage(bwa.BamToBWAchr, add, rel.One2many(['sample_name', 'chrom', 'rgid']), tags=dict(bam=[sample_name], chrom=[chrom], rgId=[rgid], chromosome_only_split=False))
        
        # Pipeline continues

        indelrealign    =  recipe.add_stage(gatk.IndelRealigner, align)

### Pipeline

    #mark_duplicates     = recipe.add_stage(tools.MarkDuplicates, parent=indel_realigner, rel=rel.Many2many(['bam', 'rgId'], ['chrom']))
    #bqsr                = recipe.add_stage(tools.BQSR, parent=mark_duplicates, rel=rel.Many2one(['bam', 'chrom']))
    #haplotype_caller    = recipe.add_stage(tools.HaplotypeCaller, parent=bqsr, rel=rel.One2one)
    #genotype_gvcfs      = recipe.add_stage(tools.GenotypeGVCFs, parent=haplotype_caller, rel=Many2one(['chrom']))
    #vqsr                = recipe.add_stage(tools.VQSR, parent=genotype_gvcfs, rel=One2many([glm, skip_VQSR]), tag={'vcf':'main'})


### Run
    
    return(recipe)

if __name__ == '__main__':
    cosmos_app = Cosmos('sqlite:///sqlite.db', default_drm='local')
    cosmos_app.initdb()

    ex = Execution.start(cosmos_app, 'Simple', 'out/simple', max_attempts=3, restart=True, skip_confirm=True)
    genomekey_main(ex)


