import os
import sys
import WGS
import logging

import jobexcutor

class workflowResolver():
    def __init__(self):
        self.workflowName=""
        self.workflowJson=""
        self.queue=""
        self.project=""
    def loadworkflow(self, workflowName,workflowJson=None):
        workflowparser=eval(workflowName).interface()
        jsoncontent={}
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
                stepc=eval(workflowName+'.'+step)
                stepc.parameter=jsoncontent[step]['parameter']
                stepc.program=jsoncontent[step]['program']
                stepc.outdirMain=jsoncontent['outdir']
                commandshell,out1,out2=stepc.makeCommand(jsoncontent[step]['input'][0],jsoncontent[step]['input'][1])
                logging.info("output:"+out1+"\n"+out2)
                runjob=jobexcutor.jobexecutor()
                runjob.runclusterjob(commandshell,stepc.outdirMain,jsoncontent[step]['resource'])

                


        




