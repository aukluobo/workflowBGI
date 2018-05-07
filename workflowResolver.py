import os
import sys
import re
import logging
from multiprocessing import Pool
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
            logging.info("fqlist is not defined; please use makejson to make a json and modified the input.then use the inputjson parameter to load the setting.")
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
        logging.info("running workflow : %s " % (workflowName))
        workflowparser=eval(workflowName).interface()
        workflowparser.outdirMain=self.outdir
        workflowparser.fqList=fq
        workflowparser.fqLink=stat
        workflowparser.ref=self.genome
        jsoncontent={}
        if dumpjson>0:
            workflowparser.dumpjson()
        else:
            if workflowJson is None:
                if self.check > 0 :
                    logging.info("END")
                    sys.exit()
                logging.info("No inputjson!! using default setting to run the workflow. Or you can use makejson mode to make a json and modified if neccesary then use the inputjson parameter")
                logging.info("writing default setting to %s" % (workflowparser.output))
                workflowparser.dumpjson()
                jsoncontent=workflowparser.loadjson()
            else:
                try:
                    logging.info("loading setting in json : %s " % (workflowJson))
                    jsoncontent=workflowparser.loadjson(inputfile=workflowJson)
                except ValueError as e:
                    raise e
            for stepL in workflowparser.step:
                jobConcurrent=[]
                for step in stepL:
                    if step in jsoncontent:
                        logging.info("running step : %s " % (step))
                        jobConcurrent.append([step,workflowName,jsoncontent])
                #
                processNum=len(jobConcurrent)
                with Pool(processNum) as pool:
                    subresult=pool.starmap(self.runJobParrallel,jobConcurrent)
                    i=0
                    for x in subresult:
                        print(x)
                        print("finish %d" % (i))
                        i+=1
                    #pool.close()
                    #pool.join()
                        
            logging.info("%s completed" % (workflowName))

    def runJobParrallel(self,step,workflowName,jsoncontent):
        stepb=eval(workflowName+'.'+step)
        stepc=stepb()
        stepc.parameter=jsoncontent[step]['parameter']
        stepc.program=jsoncontent[step]['program']
        stepc.outdirMain=jsoncontent['outdir']
        inputcode=self.checkOutput(jsoncontent[step]['input'])
        if inputcode:
            #logging.info("%s not exists" % ("\t".join(jsoncontent[step]['input'])))
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
        for subCommandShell in commandshell:
            runjob.command=subCommandShell
            runjob.runclusterjob(subCommandShell,step)

    def checkjobresource(self, resource):
        alldecimal=re.findall(r'\d+',resource)
        vf=int(alldecimal[0])
        cpu=int(alldecimal[1])
        if re.search(r'M|m',resource):
            vf=vf/1024
        return vf,cpu
    
    def checkOutput(self, out=[]):
        rcCode=0
        if type(out) is list:
            for output in out:
                rc=self.checkOutput(output)
                rcCode+=rc
        elif type(out) is dict:
            for key in out.keys():
                rc=self.checkOutput(out[key])
                rcCode+=rc
        elif type(out) is str:
            try:
                stat=os.stat(out)
                if stat.st_size == 0:
                    rcCode=1
            except:
                rcCode=1
                logging.info("File: %s is not exists. check output dir and rerun." % (output))
        else:
            print(type(out))
            logging.info("unkown type")
        return rcCode
    
                


        




