import os
import sys
import re
import WGS
import logging

import jobexcutor

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')

class workflowResolver():
    def __init__(self):
        self.workflowName=""
        self.workflowJson=""
        self.queue="st.q"
        self.project=None
        self.fqList=None

    def loadFqList(self, fqList):
        try:
            lines=open(self.fqList,mode='r').readlines()
        except IOError as e:
            raise e
        stat={}
        fq=[]
        for line in lines:
            linep=line.split()
            fq1=linep[2]
            fq1base=os.path.basename(fq1)
            fq1prefix=re.sub(r'\..*$',r'',fq1base)
            stat[fq1prefix]=linep
            fq+=[linep[2],linep[3]]
        return stat,fq

    def loadworkflow(self, workflowName,dumpjson,workflowJson=None):
        stat,fq=self.loadFqList(self.fqList)
        workflowparser=eval(workflowName).interface()
        workflowparser.fqList=fq
        workflowparser.fqLink=stat
        jsoncontent={}
        if dumpjson>0:
            workflowparser.dumpjson()
        else:
            if workflowJson is None:
                workflowparser.dumpjson()
                jsoncontent=workflowparser.loadjson()
            else:
                try:
                    jsoncontent=workflowparser.loadjson(inputfile=workflowJson)
                except ValueError as e:
                    raise e
            for step in workflowparser.step:
                if step in jsoncontent:
                    stepb=eval(workflowName+'.'+step)
                    stepc=stepb()
                    stepc.parameter=jsoncontent[step]['parameter']
                    stepc.program=jsoncontent[step]['program']
                    stepc.outdirMain=jsoncontent['outdir']
                    inputcode=self.checkOutput(jsoncontent[step]['input'])
                    if inputcode:
                        sys.exit()
                    commandshell,out=stepc.makeCommand(jsoncontent[step]['input'])
                    logging.info("output:\n"+"\n".join(out))
                    runjob=jobexcutor.jobexecutor()
                    runjob.outdir=stepc.outdirMain
                    vf,cpu=self.checkjobresource(jsoncontent[step]['resource'])
                    runjob.vf=int(vf)
                    runjob.cpu=int(cpu)
                    runjob.queue=self.queue
                    runjob.project=self.project
                    runjob.command=commandshell
                    runjob.runclusterjob(commandshell,step)

            logging.info("%s completed" % (workflowName))
    
    def checkjobresource(self, resource):
        alldecimal=re.findall(r'\d+',resource)
        vf=int(alldecimal[0])
        cpu=int(alldecimal[1])
        if re.search(r'M|m',resource):
            vf=vf/1024
        return vf,cpu
    
    def checkOutput(self, out=[]):
        rcCode=0
        for output in out:
            try:
                stat=os.stat(output)
                if stat.st_size == 0:
                    rcCode=1
            except:
                rcCode=1
                logging.info("output %s is not exists. check output dir and rerun." % (output))
        return rcCode
    
                


        




