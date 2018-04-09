import os
import sys
import re
import argparse
import logging
import json

class jobexecutor:
    def __init__(self):
        self.command="echo please set command"
        self.outdir=os.path.abspath('.')
        self.input="%s/state/state.json" %(self.outdir)
    
    def runclusterjob(self,commandshell,outdirMain,resource=1):
        pass

    def submitjob(self, parameter_list):
        pass
    def checkalive(self, sgejobid):
        pass

    def dumpjson(self, outputfile=None):
        if outputfile is None:
            outputjson=self.output
        try:
            out=open(outputjson,mode='w')
            out.write("{\n")                        
            for step in self.step:
                astep=eval(step)
                stepdict = json.dumps(astep.makedefault('need_fq1','need_fq2'))
                out.write("%s:%s\n" % (step,stepdict))
            out.write("outdir:%s" % (os.path.abspath('.')))
            out.write("}\n")
            out.close()
        except IOError as e:
            raise e
    
    def loadjson(self, inputfile=None):
        if inputfile is None:
            inputjson=self.input
        try:
            jsondict=json.load(open(inputjson,mode='r').read())
        except ValueError as e:
            raise e
        return jsondict
