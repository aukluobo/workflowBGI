import os
import sys
import re
import json
import argparse
import logging
bindir = os.path.abspath(os.path.dirname(sys.argv[0]))
sys.path.append(bindir+"/subworkflow")


logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')

import workflowResolver




def showhelp(moreinfo=""):
    help="""
    this program is the interface of the workflow excecutor. please use -h parameter to check detail
    """
    logging.info("%s%s",help,moreinfo)

def checkSpecies(species):
    if species is None:
        return {"none":"none"}
    else:
        parta=species.split(',')
        soft="none"
        specie="none"
        sdict={}
        for content in parta:
            if re.search(r'\:',content):
                contents=content.split(":")
                soft=contents[0]
                specie=contents[1]
            else:
                specie=content
            try:
                sdict[soft].append(specie)
            except:
                sdict[soft]=[specie]
        return sdict


if __name__=='__main__':
    if len(sys.argv) == 1 :
        showhelp()
        sys.exit()
    
    pwd=os.path.abspath('.')
    parser=argparse.ArgumentParser(description="pipeline annotator help")
    parser.add_argument('--mode',dest='runMode',type=str,help='how to action: run/makejson. run means executing the workflow. makejson means make a json with default parameter with defualt name')
    parser.add_argument('--workflow',dest='workflowName',type=str,help='workflow name to excute,support: WGS Repeat_Annotation RNAseq  RNAref  RNAdenovo')
    parser.add_argument('--outdir',dest='outdir',type=str,default=pwd,help='the output directory,default current directory')
    parser.add_argument('--inputjson',dest='inputjson',type=str,default=None,help='the user specified json used in workflow. if used makejson to generate and modified the json, then no need to specified')
    parser.add_argument('--clusterQueue',dest='queue',type=str,default='st.q',help='the queue used to run the workflow, -q in qsub,default st.q')
    parser.add_argument('--projectCode',dest='preject',type=str,help="the project code used to run in queue, -P in qsub. if you don't have one, don't set. but the job may not run.")
    parser.add_argument('--fqlist',dest='fqList',type=str,help="the list file that could contain five column: sampleID libraryID fq1path fq2path [specieA,specieB].[] means optional. species will used in RNAseq commparison test.")
    parser.add_argument('--genomefa',dest='genomeFa',type=str,help="the genome fa used in workflow.\n HG19:/hwfssz1/BIGDATA_COMPUTING/GaeaProject/reference/hg19/hg19.fasta\n HG38:/hwfssz1/BIGDATA_COMPUTING/GaeaProject/reference/hg38/hg38.fa")
    parser.add_argument('--species',dest='species',type=str,help="set species name where needed. format: augustus:A,genewise:B,C,D,fgene:E,F. A will used in augustus,BCD will used in genwise and so on.")
    
    localeArg=parser.parse_args()

    dumpjson=0
    if localeArg.runMode is None:
        showhelp("need set --mode")
        sys.exit()
    if localeArg.workflowName is None:
        showhelp("need set --workflow")
        sys.exit()

    if localeArg.runMode == 'makejson':
        if localeArg.fqList is None and localeArg.genomeFa is None:
            showhelp("need fqlist or genome to makejson")
            sys.exit()
        dumpjson=1
    elif localeArg.runMode == 'run':
        pass
    else:
        logging.info("unknow mode ==> %s" % (localeArg.runMode))
        sys.exit()
    
    print(localeArg)

    supportWorkflow={'workflowExample':1,'WGS':1,'RNAseq':1,'RNAref':1,'RNAdenovo':1,'Repeat_Annotation':1}
    try:
        aa=supportWorkflow[localeArg.workflowName]
    except:
        logging.info("%s is not support now! try: %s " % (localeArg.workflowName,"\t".join(supportWorkflow.keys())))
        sys.exit()
    absoutdir=os.path.abspath(localeArg.outdir)
    os.makedirs(absoutdir,mode=0o755,exist_ok=True)
    allSpecies=checkSpecies(localeArg.species)

    startw=workflowResolver.workflowResolver()
    startw.outdir=absoutdir
    startw.project=localeArg.preject
    startw.queue=localeArg.queue
    startw.fqList=localeArg.fqList
    startw.genome=localeArg.genomeFa
    startw.species=allSpecies
    startw.loadworkflow(localeArg.workflowName,dumpjson,localeArg.inputjson)





