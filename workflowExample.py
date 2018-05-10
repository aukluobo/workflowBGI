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
        pairArray=[]
        pair={}
        order={}
        p=0
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
                if prefix in order:
                    pass
                else:
                    order[prefix]=p
                p+=1
        for pp in sorted(order.items(),key=lambda x : x[1]):
            pairArray.append(pair[pp[0]])
        return pairArray

class filter(common):
    def __init__(self):
        common.__init__(self)
        self.parameter="filter -n 0.1 -q 0.5 -T 1 -l 12 -Q 2 -G -f AAGTCGGAGGCCAAGCGGTCTTAGGAAGACAA -r AAGTCGGATCGTAGCCATGTCGTTCTGTGAGCCAAGGAGTTG -M 2"
        self.program="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/soapnuke/soapnuke_v1.0/bin/SOAPnuke1.5.6"
        self.statprogram="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/soapnuke/soapnuke_v1.0/bin/soapnuke_stat.pl"
        self.outdir="WGS/cleanfq"
        
        
    def makeCommand(self,inputfq):
        pair=self.makepair(inputfq)
        command=""
        output=[]
        for keys in pair:
            readc=sorted(keys.keys())
            if len(readc)==2:
                fq1=keys[readc[0]]
                fq2=keys[readc[1]]
                fq1base=os.path.basename(fq1)
                fq2base=os.path.basename(fq2)
                fq1basesub=re.sub(r'_\d\..*',r'',fq1base)
                fq1out="%s/%s/%s.clean.fq" % (self.outdir,fq1basesub,fq1base)
                fq2out="%s/%s/%s.clean.fq" % (self.outdir,fq1basesub,fq2base)
                outdir="%s/%s" % (self.outdir,fq1basesub)
                os.makedirs(outdir,mode=0o755,exist_ok=True)
                command += "%s %s -1 %s -2 %s -o %s -C %s -D %s\n" % (self.program,self.parameter,fq1,fq2,outdir,fq1out,fq2out)
                output.append(fq1out+".gz")
                output.append(fq2out+".gz")
        return [command],output
    def makedefault(self,inputfq):
        pair=self.makepair(inputfq)
        inputd=[]
        output=[]
        for keys in pair:
            readc=sorted(keys.keys())
            if len(readc)==2:
                fq1=keys[readc[0]]
                fq2=keys[readc[1]]
                fq1base=os.path.basename(fq1)
                fq2base=os.path.basename(fq2)
                fq1basesub=re.sub(r'_\d\..*',r'',fq1base)
                fq1out="%s/%s/%s.clean.fq" % (self.outdir,fq1basesub,fq1base)
                fq2out="%s/%s/%s.clean.fq" % (self.outdir,fq1basesub,fq2base)
                inputd+=[fq1,fq2]
                output+=[fq1out+".gz",fq2out+".gz"]
        default={'input':inputd,'parameter':self.parameter,'program':self.program,'resource':"1G,1CPU",'output':output}
        return default

class alignment(common):
    def __init__(self):
        common.__init__(self)
        self.parameter=[""" mem -t 8 -M -Y -R "@RG\tID:test\tPL:COMPLETE\tPU:lib\tLB:sampleid-WGSPE100\tSM:PE100-2\tCN:BGI" """]
        self.program="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/bin/bwa-0.7.15/bwa"
        self.samtools="/ldfssz1/BC_WGS/pipeline/DNA_Human_WGS_2017b/bin/samtools-1.3.1/samtools"      
        self.outdir="WGS/bam"

    def makeCommand(self,inputfq):
        pair=self.makepair(inputfq)
        command=""
        output=[]
        readGroupOrder=0
        if len(self.parameter) != len(pair):
            print("read pair and parameter group not equal! will use the last parameter for default!")
        maxReadGroup=len(self.parameter)
        for keys in pair:
            readc=sorted(keys.keys())
            if len(readc)==2:
                fq1=keys[readc[0]]
                fq2=keys[readc[1]]
                outprefix=os.path.basename(fq1).split('.')
                parameter1=self.parameter[readGroupOrder]
                #parameter1=parameter.replace("test",outprefix[0]).replace("lib",self.fqLink[outprefix[0]][1]).replace("sampleid",self.fqLink[outprefix[0]][0])
                outputbam="%s/%s.bam" % (self.outdir,outprefix[0])
                command+="%s %s %s %s %s | %s view -Sb -o %s - ;" % (self.program,parameter1,self.ref,fq1,fq2,self.samtools,outputbam)
                command+="%s sort -@ 8 -O BAM -o %s.sort.bam %s ; %s index %s.sort.bam\n" % (self.samtools,outputbam,outputbam,self.samtools,outputbam)
                output+=[outputbam,outputbam+".sort.bam"]
            readGroupOrder+=1
            if readGroupOrder>maxReadGroup:
                readGroupOrder=maxReadGroup
        os.makedirs(self.outdir,mode=0o755,exist_ok=True)
        return [command],output
    
    def makedefault(self,inputfq):
        pair=self.makepair(inputfq)
        inputq=[]
        output=[]
        parameter=[]
        for keys in pair:
            readc=sorted(keys.keys())
            if len(readc)==2:
                fq1=keys[readc[0]]
                outprefix=os.path.basename(fq1).split('.')
                parameter1=self.parameter[0].replace("test",outprefix[0]).replace("lib",self.fqLink[outprefix[0]][1]).replace("sampleid",self.fqLink[outprefix[0]][0])
                parameter.append(parameter1)
                outputbam="%s/%s.bam" % (self.outdir,outprefix[0])
                output+=[outputbam,outputbam+".sort.bam"]
        fi=filter()
        fi.outdir=self.outdir
        out1,inputq=fi.makeCommand(inputfq)
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
                    astepo.ref=self.ref
                    stepdict = json.dumps(astepo.makedefault(self.fqList))
                    #stepdict = json.dumps(astepjson)
                    out.write("\"%s\":%s,\n" % (step,stepdict))
            out.write("\"ref\":\"%s\",\n" % (self.ref))
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



