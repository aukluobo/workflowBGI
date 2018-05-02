import os
import sys
import json
import re

class common(object):
    def __init__(self):
        self.fqList=['need_fq1','need_fq2']
        self.fqLink={}
        self.outdirMain=os.path.abspath('.')
        self.ref="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/Database/hg19/hg19.fasta"

    def makepair(self, inputfq):
        pair={}
        for fq in inputfq:
            fqbase=os.path.basename(fq)
            aa=re.search(r'_\d\.fq',fqbase)
            if aa:
                prefix=fqbase[0:aa.start()]
                readtype=int(fqbase[aa.start()+1])
                try:
                    pair[prefix][readtype]=fq
                except:
                    pair[prefix]={readtype:fq}
        return pair

class filter(common):
    def __init__(self):
        common.__init__(self)
        self.parameter="filter -n 0.1 -q 0.5 -T 1 -l 12 -Q 2 -G -f AAGTCGGAGGCCAAGCGGTCTTAGGAAGACAA -r AAGTCGGATCGTAGCCATGTCGTTCTGTGAGCCAAGGAGTTG -M 2"
        self.program="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/soapnuke/soapnuke_v1.0/bin/SOAPnuke1.5.6"
        self.statprogram="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/soapnuke/soapnuke_v1.0/bin/soapnuke_stat.pl"
        self.outdir="%s/WGS/cleanfq" % (self.outdirMain)
        
        
    def makeCommand(self,inputfq):
        pair=self.makepair(inputfq)
        command=""
        output=[]
        for keys in pair.keys():
            readc=sorted(pair[keys].keys())
            if len(readc)==2:
                fq1=pair[keys][readc[0]]
                fq2=pair[keys][readc[1]]
                fq1base=os.path.basename(fq1)
                fq2base=os.path.basename(fq2)
                fq1out="%s/%s.clean.fq" % (self.outdir,fq1base)
                fq2out="%s/%s.clean.fq" % (self.outdir,fq2base)
                command += "%s %s -1 %s -2 %s -o %s -C %s -D %s\n" % (self.program,self.parameter,fq1,fq2,self.outdir,fq1out,fq2out)
                output.append(fq1out+".fq.gz")
                output.append(fq2out+".fq.gz")
        os.makedirs(self.outdir,mode=0o755,exist_ok=True)
        return [command],output
    def makedefault(self,inputfq):
        pair=self.makepair(inputfq)
        inputd=[]
        output=[]
        for keys in pair.keys():
            readc=sorted(pair[keys].keys())
            if len(readc)==2:
                fq1=pair[keys][readc[0]]
                fq2=pair[keys][readc[1]]
                fq1base=os.path.basename(fq1)
                fq2base=os.path.basename(fq2)
                fq1out="%s/%s.clean.fq" % (self.outdir,fq1base)
                fq2out="%s/%s.clean.fq" % (self.outdir,fq2base)
                inputd+=[fq1,fq2]
                output+=[fq1out+".fq.gz",fq2out+".fq.gz"]
        default={'input':inputd,'parameter':self.parameter,'program':self.program,'resource':"1G,1CPU",'output':output}
        return default

class alignment(common):
    def __init__(self):
        common.__init__(self)
        self.parameter=""" mem -t 8 -M -Y -R "@RG\tID:test\tPL:COMPLETE\tPU:lib\tLB:sampleid-WGSPE100\tSM:PE100-2\tCN:BGI" """
        self.program="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/bin/bwa-0.7.15/bwa"
        self.samtools="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/bin/samtools-1.3.1/samtools"      
        self.outdir="%s/WGS/bam" % (self.outdirMain)

    def makeCommand(self,inputfq):
        pair=self.makepair(inputfq)
        command=""
        output=[]
        for keys in pair.keys():
            readc=sorted(pair[keys].keys())
            if len(readc)==2:
                fq1=pair[keys][readc[0]]
                fq2=pair[keys][readc[1]]
                outprefix=os.path.basename(fq1).split('.')
                parameter=self.parameter
                parameter1=parameter.replace("test",outprefix[0]).replace("lib",self.fqLink[outprefix[0]][1]).replace("sampleid",self.fqLink[outprefix[0]][0])
                outputbam="%s/%s.bam" % (self.outdir,outprefix[0])
                command+="%s %s %s %s %s | %s view -Sb -o %s - ;" % (self.program,parameter1,self.ref,fq1,fq2,self.samtools,outputbam)
                command+="%s sort -@ 8 -O BAM -o %s.sort.bam %s ; %s index %s.sort.bam\n" % (self.samtools,outputbam,outputbam,self.samtools,outputbam)
                output+=[outputbam,outputbam+".sort.bam"]
        os.makedirs(self.outdir,mode=0o755,exist_ok=True)
        return [command],output
    
    def makedefault(self,inputfq):
        pair=self.makepair(inputfq)
        inputq=[]
        output=[]
        parameter=[]
        for keys in pair.keys():
            readc=sorted(pair[keys].keys())
            if len(readc)==2:
                fq1=pair[keys][readc[0]]
                fq2=pair[keys][readc[1]]
                outprefix=os.path.basename(fq1).split('.')
                parameter1=self.parameter.replace("test",outprefix[0]).replace("lib",self.fqLink[outprefix[0]][1]).replace("sampleid",self.fqLink[outprefix[0]][0])
                parameter.append(parameter1)
                outputbam="%s/%s.bam" % (self.outdir,outprefix[0])
                output+=[outputbam,outputbam+".sort.bam"]
                inputq+=[fq1,fq2]
        default={'input':inputq,'parameter':parameter,'program':self.program,'resource':"12G,8CPU",'output':output}
        return default                        
class piler(common):
    def __init__(self, parameter_list):
        pass



class interface(common):
    def __init__(self):
        common.__init__(self)
        self.step=[["filter"],["alignment"]]
        self.input="%s/workflow.json" % (self.outdirMain)
        self.output="%s/workflow.json" % (self.outdirMain)
        #self.input=""
        #self.output=""
        
        
    def dumpjson(self, outputfile=None):
        #self.output="%s/workflow.json" % (self.outdirMain)
        outputjson=self.output
        if outputfile is not None:
            outputjson=outputfile
        try:
            out=open(outputjson,mode='w')
            out.write("{\n")                        
            for stepL in self.step:
                #astepjson={}
                #if step == "filter":
                #    aa=filter()
                #    astepjson=aa.makedefault("need_fq1","need_fq2")
                #elif step == "alignment":
                #    aa=alignment()
                #    astepjson=aa.makedefault("need_fq1","need_fq2")
                #else:
                #    pass
                for step in stepL:
                    astep=eval(step)
                    astepo=astep()
                    astepo.fqLink=self.fqLink
                    stepdict = json.dumps(astepo.makedefault(self.fqList))
                    #stepdict = json.dumps(astepjson)
                    out.write("\"%s\":%s,\n" % (step,stepdict))
            out.write("\"outdir\":\"%s\"\n" % (self.outdirMain))
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



