import os
import sys
import json

class filter:
    def __init__(self):
        self.parameter="filter -n 0.1 -q 0.5 -T 1 -l 12 -Q 2 -G -f AAGTCGGAGGCCAAGCGGTCTTAGGAAGACAA -r AAGTCGGATCGTAGCCATGTCGTTCTGTGAGCCAAGGAGTTG -M 2"
        self.program="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/soapnuke/soapnuke_v1.0/bin/SOAPnuke1.5.6"
        self.statprogram="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/soapnuke/soapnuke_v1.0/bin/soapnuke_stat.pl"
        self.outdirMain=os.path.abspath('.')
        self.outdir="%s/WGS/cleanfq" % (self.outdirMain)
        
    
    def makeCommand(self,fq1,fq2):
        fq1base=os.path.basename(fq1)
        fq2base=os.path.basename(fq2)
        fq1out="%s/%s.clean.fq" % (self.outdir,fq1base)
        fq2out="%s/%s.clean.fq" % (self.outdir,fq2base)
        command = "%s %s -1 %s -2 %s -o %s -C %s -D %s" % (self.program,self.parameter,fq1,fq2,self.outdir,fq1out,fq2out)
        print(self.parameter)
        os.makedirs(self.outdir,mode=0o755,exist_ok=True)
        return command,fq1out,fq2out
    def makedefault(self,fq1,fq2):
        fq1base=os.path.basename(fq1)
        fq2base=os.path.basename(fq2)
        fq1out="%s/%s.clean.fq" % (self.outdir,fq1base)
        fq2out="%s/%s.clean.fq" % (self.outdir,fq2base)
        default={'input':[fq1,fq2],'parameter':self.parameter,'program':self.program,'resource':"1G,1CPU",'output':[fq1out+".fq.gz",fq2out+".fq.gz"]}
        return default

class alignment:
    def __init__(self):
        self.parameter=""" mem -t 8 -M -Y -R "@RG\tID:test\tPL:COMPLETE\tPU:lib\tLB:sampleid-WGSPE100\tSM:PE100-2\tCN:BGI" """
        self.program="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/bin/bwa-0.7.15/bwa"
        self.samtools="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/bin/samtools-1.3.1/samtools"
        self.ref="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/Database/hg19/hg19.fasta"
        self.outdirMain=os.path.abspath('.')
        self.outdir="%s/WGS/bam" % (self.outdirMain)

    def makeCommand(self, fq1,fq2):
        outprefix=os.path.basename(fq1).split('.')
        outputbam="%s/%s.bam" % (self.outdir,outprefix[0])
        command="%s %s %s %s %s | %s view -Sb -o %s -\n" % (self.program,self.parameter,self.ref,fq1,fq2,self.samtools,outputbam)
        commands="%s sort -@ 8 -O BAM -o %s.sort.bam %s\n%s index %s.sort.bam" % (self.samtools,outputbam,outputbam,self.samtools,outputbam)
        os.makedirs(self.outdir,mode=0o755,exist_ok=True)
        return command+commands,outputbam,outputbam+".sort.bam"
    
    def makedefault(self,fq1,fq2):
        outprefix=os.path.basename(fq1).split('.')
        outputbam="%s/%s.bam" % (self.outdir,outprefix[0])
        default={'input':[fq1,fq2],'parameter':self.parameter,'program':self.program,'resource':"12G,8CPU",'output':outputbam+".sort.bam"}
        return default                        



class interface():
    def __init__(self):
        self.step=("filter","alignment")
        self.input="%s/workflow.json" % (os.path.abspath('.'))
        self.output="%s/workflow.json" % (os.path.abspath('.'))

    def dumpjson(self, outputfile=None):
        outputjson=self.output
        if outputfile is not None:
            outputjson=outputfile
        try:
            out=open(outputjson,mode='w')
            out.write("{\n")                        
            for step in self.step:
                #astepjson={}
                #if step == "filter":
                #    aa=filter()
                #    astepjson=aa.makedefault("need_fq1","need_fq2")
                #elif step == "alignment":
                #    aa=alignment()
                #    astepjson=aa.makedefault("need_fq1","need_fq2")
                #else:
                #    pass
                astep=eval(step)
                astepo=astep()
                stepdict = json.dumps(astepo.makedefault('need_fq1','need_fq2'))
                #stepdict = json.dumps(astepjson)
                out.write("\"%s\":%s,\n" % (step,stepdict))
            out.write("\"outdir\":\"%s\"\n" % (os.path.abspath('.')))
            out.write("}\n")
            out.close()

        except IOError as e:
            raise e
    
    def loadjson(self, inputfile=None):
        inputjson=self.input
        if inputfile is not None:
            inputjson=inputfile
        try:
            infl=open(inputjson,mode='r')
            jsondict=json.load(infl)
        except ValueError as e:
            raise e
        return jsondict



