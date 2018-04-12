import os
import sys
import re
import json
import argparse
import logging

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')

import workflowResolver




def showhelp(moreinfo=""):
    help="""
    this program is the interface of the workflow excecutor. please use -h parameter to check detail
    """
    logging.info("%s%s",help,moreinfo)

if __name__=='__main__':
    if len(sys.argv) == 1 :
        showhelp()
        sys.exit()
    
    pwd=os.path.abspath('.')
    parser=argparse.ArgumentParser(description="pipeline annotator help")
    parser.add_argument('--mode',dest='runMode',type=str,help='how to action: run/makejson. run means executing the workflow. makejson means make a json with default parameter with defualt name')
    parser.add_argument('--workflow',dest='workflowName',type=str,help='workflow name to excute,support: WGS')
    parser.add_argument('--outdir',dest='outdir',type=str,default=pwd,help='the output directory,default current directory')
    parser.add_argument('--inputjson',dest='inputjson',type=str,default=None,help='the user specified json used in workflow. if used makejson to generate and modified the json, then no need to specified')
    parser.add_argument('--clusterQueue',dest='queue',type=str,default='st.q',help='the queue used to run the workflow, -q in qsub,default st.q')
    parser.add_argument('--projectCode',dest='preject',type=str,help='the project code used to run in queue, -P in qsub')

    localeArg=parser.parse_args()

    dumpjson=0
    if localeArg.runMode is None:
        showhelp("need set --mode")
        sys.exit()
    if localeArg.workflowName is None:
        showhelp("need set --workflow")
        sys.exit()
    if localeArg.runMode == 'makejson':
        dumpjson=1
    
    print(localeArg)
    print(dumpjson)

    startw=workflowResolver.workflowResolver()
    startw.project=localeArg.preject
    startw.queue=localeArg.queue
    startw.loadworkflow(localeArg.workflowName,dumpjson,localeArg.inputjson)





