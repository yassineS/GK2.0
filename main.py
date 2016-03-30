import os

from cosmos import Execution, add_execution_args, Cosmos
from genomekey import genomekey_main


#########################
# File naming
#########################

def _task_output_dir(task):
    out = task.execution.output_dir
    m   = lambda reg:bool(re.search(reg, task.stage.name))

    return os.path.join(out,task.tags['bam'],task.tags['chrom'])


#########################
# bam
#########################

#def bam(input_bam,input_bam_list,test_bam,high_coverage,chromosome_only_split,**kwargs):
def bam(execution, kwargs):
    
    #Input file is a bam with properly annotated readgroups.
    print kwargs
    input_bam = kwargs['input_bam']
    input_bam_list = kwargs['input_list']
    high_coverage = kwargs['high_coverage']
    test_bam = kwargs['test_bam']
    chromosome_only_split = kwargs['chromosome_only_split']
                            
    input_bams = input_bam_list.read().strip().split('\n') if input_bam_list else []

    if input_bam:
        input_bams.append(input_bam.name)

    if len(input_bams) == 0:
        raise WorkflowException, 'At least 1 BAM input required'

    # if we have a high coverage genome, override GATK options for realignment
    if high_coverage:
        settings['gatk_realigntarget'] = '--mismatchFraction 0.30 --maxIntervalSize 650'
        settings['gatk_indelrealign'] = '--maxReadsInMemory 300000 --maxReadsForRealignment 500000 --maxReadsForConsensuses 500 --maxConsensuses 100'

    #test_bam = test_bam
    #chromosome_only_split = chromosome_only_split

    recipe = genomekey_main(input_bams, test_bam=test_bam, chromosome_only_split=chromosome_only_split)
    execution.run(recipe, task_output_dir=_task_output_dir)

   
#########################
# CLI Configuration
########################
def main():
    cosmos_app = Cosmos('sqlite:///sqlite.db', default_drm='local')
    cosmos_app.initdb()
    
    import argparse

    parser = argparse.ArgumentParser(description='WGA Pipeline')
    
    parser.add_argument('-g','--growl', action='store_true', help='sends a growl notification on execution status changes')
    sps = parser.add_subparsers(title="Commands", metavar="<command>")

    sp = sps.add_parser('resetdb', help=cosmos_app.resetdb.__doc__)
    sp.set_defaults(func=cosmos_app.resetdb)

    sp = sps.add_parser('initdb', help=cosmos_app.initdb.__doc__)
    sp.set_defaults(func=cosmos_app.initdb)

    sp = sps.add_parser('shell', help=cosmos_app.shell.__doc__)
    sp.set_defaults(func=cosmos_app.shell)

    sp = sps.add_parser('runweb', help=cosmos_app.runweb.__doc__)
    sp.add_argument('-p', '--port', type=int, help='port to bind the server to')
    sp.add_argument('-H', '--host', default='localhost', help='host to bind the server to')
    sp.set_defaults(func=cosmos_app.runweb)

    bam_sp = sps.add_parser('bam', help='Whole Genome Analysis Pipeline')
    bam_sp.add_argument('-i', '--input_bam', type=file, help='Path to a BAM file, the index file .bai should be in the same directory with the same name')
    bam_sp.add_argument('-il', '--input_list', type=file, help='Path to a file countaining a list of paths to BAMs, seperated by newlines')
    bam_sp.add_argument('-hc', '--high_coverage', action='store_true', help='Special GATK options to handle high-coverage genomes')
    bam_sp.add_argument('-co', '--chromosome_only_split', action='store_true', help='Split only on chromosomes not read groups')
    bam_sp.add_argument('-t', '--test_bam', action='store_true', help='Only do stages on chromosome 1, skips VQSR, strictly for testing')
    bam_sp.add_argument('-n', '--name',type=str,help='Pipeline name.',required=True)
    bam_sp.set_defaults(func=bam)
    #bam_sp.set_defaults(func=genomekey.genomekey_main)
    add_execution_args(bam_sp)


    args = parser.parse_args()
    kwargs = dict(args._get_kwargs())
    func = kwargs.pop('func')
    growl = kwargs.pop('growl')
    pipe_name=kwargs.pop('name')

    if growl:
       from cosmos.util import growl
       from cosmos import signal_execution_status_change, ExecutionStatus
       @signal_execution_status_change.connect
       def growl_signal(execution):
           if execution.status != ExecutionStatus.running:
               growl.send('%s %s' % (execution, execution.status))

    #if func.__name__.startswith('ex'):
    #  execution_params = {n: kwargs.pop(n, None) for n in ['name', 'restart', 'skip_confirm', 'max_cpus', 'max_attempts', 'output_dir']}
    #  if not execution_params['output_dir']:
    #     execution_params['output_dir'] = os.path.join(root_path, 'out', execution_params['name'])

      #ex = Execution.start(cosmos_app=cosmos_app, **execution_params)
    ex = Execution.start(cosmos_app=cosmos_app, output_dir=os.path.join('out/genomekey',pipe_name), name=pipe_name, restart=True, max_attempts=2)
    bam(ex, kwargs)
    kwargs['execution'] = ex
    func(**kwargs)

if __name__ =='__main__':

    main()
    
