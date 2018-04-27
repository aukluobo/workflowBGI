import os
import sys
import re
import logging

import jobexcutor
import workflowExample

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')

class workflowResolver():
    def __init__(self):
        self.workflowName=""
        self.workflowJson=""
        self.queue="st.q"
        self.outdir=os.path.abspath('.')
        self.project=None
        self.fqList=None
        self.genome=None
        self.check=0

    def loadFqList(self, fqList):
        if fqList is None:
            logging.info("fqlist is not defined; please use makejson to make a json and modified the input")
            self.check=1
            return ["test_1.fq.gz","test_2.fq.gz"],{'a':["1","2","3","4"]}
        else:
            try:
                lines=open(fqList,mode='r').readlines()
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
        workflowparser.outdirMain=self.outdir
        workflowparser.fqList=fq
        workflowparser.fqLink=stat
        workflowparser.ref=self.genome
        jsoncontent={}
        if dumpjson>0:
            workflowparser.dumpjson()
        else:
            if self.check > 0 :
                logging.info("END")
                sys.exit()
            if workflowJson is None:
                logging.info("use makejson mode to make a json and modified if neccesary then use the inputjson parameter")
                sys.exit()
            else:
                try:
                    jsoncontent=workflowparser.loadjson(inputfile=workflowJson)
                except ValueError as e:
                    raise e
            for stepL in workflowparser.step:
                if len(stepL)==1:  #temporarily writing. will use multiprocess modoule to fix parallel steps
                    step=stepL[0]
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
    
                


        




