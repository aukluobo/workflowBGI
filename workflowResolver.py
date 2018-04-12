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
    
    def loadworkflow(self, workflowName,dumpjson,workflowJson=None):
        workflowparser=eval(workflowName).interface()
        jsoncontent={}
        if dumpjson>0:
            workflowparser.dumpjson()
        else:
            if workflowJson is None:
                #workflowparser.dumpjson()
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
                    commandshell,out1,out2=stepc.makeCommand(jsoncontent[step]['input'][0],jsoncontent[step]['input'][1])
                    logging.info("output:"+out1+"\n"+out2)
                    runjob=jobexcutor.jobexecutor()
                    runjob.outdir=stepc.outdirMain
                    vf,cpu=self.checkjobresource(jsoncontent[step]['resource'])
                    runjob.vf=int(vf)
                    runjob.cpu=int(cpu)
                    runjob.queue=self.queue
                    runjob.project=self.project
                    runjob.runclusterjob(commandshell,step)
            logging.info("%s completed" % (workflowName))
    
    def checkjobresource(self, resource):
        alldecimal=re.findall(r'\d+',resource)
        vf=int(alldecimal[0])
        cpu=int(alldecimal[1])
        if re.search(r'M|m',resource):
            vf=vf/1024
        return vf,cpu
                


        




