import os
import sys
import WGS

#import jobexcutor

class workflowResolver():
    def __init__(self):
        self.workflowName=""
        self.workflowJson=""
        self.queue=""
        self.project=""
        self.wgs=WGS()
        self.allflow={'WGS':self.wgs}
    def loadworkflow(self, workflowName,workflowJson=None):
        workflowparser=allflow[workflowName]
        if workflowJson is None:
            workflowparser.dumpjson()
        else:
            workflowparser.loadjson()




