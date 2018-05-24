import os
import sys
import re
import logging
import json
from multiprocessing import Pool
import jobexcutor
import workflowExample
import Repeat_Annotation,WGS,RNAseq

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
        self.species={}
        self.check=0
        self.stat={}
        self.fq=[]
        self.jsonOutput="workflow.json"

    def loadFqList(self, fqList):
        if fqList is None:
            #logging.info("fqlist is not defined; please use makejson to make a json and modified the input.then use the inputjson parameter to load the setting.")
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
                fq1prefix=re.sub(r'_\d\..*$',r'',fq1base)
                stat[fq1prefix]=linep
                fq+=[linep[2],linep[3]]
            return stat,fq
    
    def loadworkflow(self, workflowName,dumpjson,workflowJson=None):
        self.stat,self.fq=self.loadFqList(self.fqList)
        logging.info("running workflow : %s " % (workflowName))
        workflowparser=eval(workflowName).interface()
        allStep=workflowparser.step
        jsoncontent={}
        if dumpjson>0:
            self.dumpjson(allStep,workflowName)
        else:
            if workflowJson is None:
                if self.check > 0 and self.genome is None :
                    logging.info(" Not enough input. workflow end.")
                    sys.exit()
                logging.info("No inputjson!! using default setting to run the workflow. Or you can use makejson mode to make a json and modified if neccesary then use the inputjson parameter")
                logging.info("writing default setting to %s/%s" % (self.outdir,self.jsonOutput))
                self.dumpjson(allStep,workflowName)
                jsoncontent=self.loadjson()
            else:
                try:
                    logging.info("loading setting in json : %s " % (workflowJson))
                    jsoncontent=self.loadjson(workflowJson)
                except ValueError as e:
                    raise e
            for stepL in allStep:
                jobConcurrent=[]
                stepname=[]
                for step in stepL:
                    if step in jsoncontent:
                        logging.info("running step : %s " % (step))
                        jobConcurrent.append([step,workflowName,jsoncontent])
                        stepname.append(step)
                    else:
                        logging.info(step+" not in json")
                #
                if jobConcurrent:
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
                    #merge stat.json in to one stat.json
                    runjob=jobexcutor.jobexecutor()
                    runjob.outdir=jsoncontent['outdir']
                    runjob.input=runjob.outdir+"/state/allstate.json"
                    totaljson={}
                    for element in stepname:
                        inputfile=runjob.outdir+"/state/"+element+"state.json"
                        stepdict=runjob.loadjson(inputfile)
                        totaljson.update(stepdict)
                    runjob.dumpjson(totaljson)
                else:
                    logging.info("no step need run in %s " % ("".join(stepL)))
                        
            logging.info("%s completed" % (workflowName))

    def runJobParrallel(self,step,workflowName,jsoncontent):
        stepb=eval(workflowName+'.'+step)
        stepc=stepb()
        stepc.parameter=jsoncontent[step]['parameter']
        stepc.program=jsoncontent[step]['program']
        stepc.outdirMain=jsoncontent['outdir']
        stepc.species=self.species
        stepc.fqList=self.fq
        stepc.fqLink=self.stat
        stepc.ref=self.genome
        stepc.outdir=stepc.outdirMain+"/"+stepc.outdir
        inputcode=self.checkOutput(jsoncontent[step]['input'])
        if inputcode:
            #logging.info("%s not exists" % ("\t".join(jsoncontent[step]['input'])))
            sys.exit()
        commandshell,out=stepc.makeCommand(jsoncontent[step]['input'])
        logging.info("output: %s\n" % (out))
        runjob=jobexcutor.jobexecutor()
        runjob.outdir=stepc.outdirMain
        runjob.input=runjob.outdir+"/state/"+step+"state.json"
        vf,cpu=self.checkjobresource(jsoncontent[step]['resource'])
        runjob.vf=int(vf)
        runjob.cpu=int(cpu)
        runjob.queue=self.queue
        runjob.project=self.project
        expectLine=len(commandshell)
        logging.info("expect %d commandline in step %s " % (expectLine,step))
        for subCommandShell in commandshell:
            runjob.command=subCommandShell
            runjob.runclusterjob(subCommandShell,step)
        outputcode=self.checkOutput(jsoncontent[step]['output'])
        if outputcode:
            print(step+" failed")

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
                logging.info("File: %s is not exists. check input file or pre output dir and rerun." % (output))
        else:
            print(type(out))
            logging.info("unkown type")
            rcCode=1
        return rcCode
    
    def dumpjson(self,allStep,workflowName):
        output="%s/%s" % (self.outdir,self.jsonOutput)
        try:
            out=open(output,mode='w')
        except IOError as e:
            raise e
        allJson={}
        for stepa in allStep:
            for stepb in stepa:
                getStep=eval(workflowName+"."+stepb)
                getStepDone=getStep()
                getStepDone.outdirMain=self.outdir
                getStepDone.fqList=self.fq
                getStepDone.fqLink=self.stat
                getStepDone.ref=self.genome
                getStepDone.outdir=getStepDone.outdirMain+"/"+getStepDone.outdir
                default=getStepDone.makedefault(self.fq)
                allJson[stepb]=default
        allJson["ref"]=self.genome
        allJson["outdir"]=self.outdir
        json.dump(allJson,out,indent=4)
        out.close()

    def loadjson(self, inputjson=None):
        inputfile="%s/%s" % (self.outdir,self.jsonOutput)
        if inputjson is not None:
            inputfile=inputjson
        try:
            jsondict=json.load(open(inputfile,mode='r'))
        except IOError as e:
            raise e
        return jsondict
                


        




