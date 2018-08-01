import os
import sys
import re
import subprocess
import argparse
import logging
import json
import time

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')

class jobexecutor:
    def __init__(self):
        self.command="echo please set command"
        self.outdir=os.path.abspath('.')
        self.input="state.json"
        self.output=self.input
        self.vf=1
        self.cpu=1
        self.queue="st.q"
        self.project=None
        self.lineSplit=100
        self.partAll=[]
    
    def runclusterjob(self,commandshell=None,jobname=None):
        #self.input=self.outdir+"/state/"+self.input
        self.output=self.input
        useCommand=commandshell
        if commandshell is None:
            useCommand=self.command
        takecommand,part=self.makeRunCommand(useCommand)
        self.partAll=[]
        for i in range(1,part+1,1):
            self.partAll.append(str(i))
        usedjobname=jobname
        os.makedirs("%s/state" % (self.outdir),mode=0o755,exist_ok=True)
        
        if jobname is None:
            usedjobname="test"
        statedict=self.loadjson()
        if statedict['control'] == 'run':
            globalcode=1
            if usedjobname in statedict:
                prejobidPart=statedict[usedjobname]
                if prejobidPart == 'completed':
                    globalcode=0
                else:
                    prejobid,partRecord=prejobidPart.split('-')
                    partRecordCheck=partRecord.split(',')
                    shellcode=self.checkshell(takecommand,"%s/shell/%s/%s.sh" % (self.outdir,jobname,jobname))
                    #check previous shell and current shell if different,then kill job and run new shell
                    #if shell code is not the same then need to rerun de job anyway
                    if shellcode==1:
                        self.killjob(prejobid)
                        if prejobid != 'completed':
                            logging.info("kill previous %s" % (usedjobname))
                    else:
                        if prejobid == 'completed':
                            globalcode=0
                        else:
                            palivecode=self.checkalive(prejobid)
                            if palivecode==0:
                                globalcode,unfinishPart=self.checkcomplete(prejobid,partRecordCheck,usedjobname)
                                if unfinishPart:
                                    uniquePart={}
                                    for pp in unfinishPart:
                                        uniquePart[pp]=1
                                    self.partAll=uniquePart.keys()
                                    
                           
            while(globalcode > 0):
                recode,submitid=self.submitjob(takecommand,usedjobname)
                timecap=30
                if recode == 0:
                    statedict[usedjobname]=submitid+"-"+",".join(self.partAll)
                elif recode > 0:
                    logging.info("resubmit job because the first submit fail")
                    resubmitid=self.rerunjob(recode,timecap,takecommand,usedjobname)
                    statedict[usedjobname]=resubmitid+"-"+",".join(self.partAll)
                else:
                    globalcode=-1
                    statedict[usedjobname]=submitid

                self.dumpjson(statedict)
                time.sleep(30)
                if globalcode > 0:
                    jobid=statedict[usedjobname].split('-')[0]
                    alivecode=self.checkalive(jobid)
                    if alivecode==0:
                        globalcode,unfinishParta=self.checkcomplete(jobid,self.partAll,usedjobname)
                        if unfinishParta:
                            uniqueParta={}
                            for ppp in unfinishParta:
                                uniqueParta[ppp]=1
                            self.partAll=uniqueParta.keys()
                    else:
                        self.killjob(jobid)
            if globalcode==0:
                statedict[usedjobname]='completed-'
            self.dumpjson(statedict)
        elif statedict['control'] == 'hold':
            self.hodljob()
        elif statedict['control'] == 'stop':
            pass
        else:
            pass
            


    def submitjob(self,commandshell,jobname):
        shelldir="%s/shell/%s" % (self.outdir,jobname)
        os.makedirs(shelldir,mode=0o755,exist_ok=True)
        out=open(shelldir+"/"+jobname+".sh",mode='w')
        out.write(commandshell)
        out.close()
        part=",".join(self.partAll)
        qsubcommand="qsub -clear -terse -N %s.sh -t %s -wd %s -l vf=%sG,num_proc=%d -binding linear:%d -q %s " % (jobname,part,shelldir,self.vf,self.cpu,self.cpu,self.queue)
        if self.project is not None:
            qsubcommand+="-P %s" % (self.project)
        qsubcommand+=" %s/%s.sh" % (shelldir,jobname)
        logging.info(qsubcommand)
        submit=subprocess.Popen(qsubcommand,shell=True,stderr=subprocess.PIPE,stdout=subprocess.PIPE,universal_newlines=True)
        returncode=submit.wait(timeout=120)
        if returncode == 0:
            stdout=submit.stdout.read()
            stderr=submit.stderr.read()
            #print(stdout+"\n"+stderr)
            if stderr is not '':
                return -1,stderr
            else:
                return returncode,re.sub(r'\..*',r'',stdout.replace('\n',''))
        else:
            return 1,None
    
    def rerunjob(self,code,timecap,commandshell,jobname):
        tmpcode=code
        tmpsid=''
        totaltime=0
        while(tmpcode != 0):
            time.sleep(timecap)
            tmpcode,tmpsid=self.submitjob(commandshell,jobname)
            totaltime+=30
            if totaltime > 7200:
                tmpcode=0
                tmpsid='fail to resubmit job for two hours.'

        return tmpsid

    def killjob(self,sgejobid):
        if sgejobid=='completed':
            pass
        else:
            cmd='qdel %s' % (sgejobid)
            subprocess.Popen(cmd,shell=True,universal_newlines=True)

    def makeRunCommand(self, command):
        commandLine=re.sub(r'\s+$',r'',command).split('\n')
        commandLineNum=len(commandLine)
        modifiedCmd=[]
        jobcu=1
        if commandLineNum > 1:
            partNum=1
            if commandLineNum >self.lineSplit:
                partNum=int(commandLineNum/self.lineSplit)+1
            elif commandLineNum<self.lineSplit:
                if self.lineSplit != 100:
                    partNum=self.lineSplit
            cu=0
            
            for line in commandLine:
                bline=re.sub(r'\s+$',r'',line)
                cline=re.sub(r';+$',r'',bline)
                lineM="if [ $SGE_TASK_ID -eq %d ];then { %s ; } && echo JobFinished $SGE_TASK_ID;fi" % (jobcu,cline)
                modifiedCmd.append(lineM)
                cu+=1
                if cu >= partNum:
                    cu=0
                    jobcu+=1
        else:
            bsline=re.sub(r'\s+$',r'',commandLine[0])
            btline=re.sub(r';+$',r'',bsline)
            lineM="if [ $SGE_TASK_ID -eq 1 ];then { %s ; } && echo JobFinished $SGE_TASK_ID;fi" % (btline)
            modifiedCmd.append(lineM)
            jobcu+=1
        spart=jobcu-1
        logging.info("%d line(s) in this step" % (spart))
        return "\n".join(modifiedCmd),spart


    def checkshell(self, newshell,oldshellfile):
        #logging.info(oldshellfile)
        oldshell=open(oldshellfile,mode='r').readlines()
        totalshell=''
        for line in oldshell:
            #logging.info(line)
            totalshell+=re.sub(r'\s+',r'',line)
        tnewshell=re.sub(r'\s+',r'',newshell)
        if tnewshell != totalshell:
            logging.info("%s\nNOT EUQAL%s\n" % (tnewshell,totalshell))
            return 1
        else:
            return 0

    def hodljob(self):
        pass
    def checkalive(self, sgejobid):
        cplcode=-1
        counttime={}
        tcount=0
        logging.info("checkalive "+sgejobid)
        while(cplcode<0):
            time.sleep(120)
            qstatuser="qstat"
            statall=subprocess.Popen(qstatuser,shell=True,stderr=subprocess.PIPE,stdout=subprocess.PIPE,universal_newlines=True)
            returncode=-1
            try:
                returncode=statall.wait(timeout=120)
            except:
                pass
            if returncode >=0:
                stdoutall=statall.stdout.readlines()
                stderrall=statall.stderr.read()
                if stderrall is not '':
                    pass
                else:
                    jobrecord=[x for x in stdoutall if re.match(re.escape(sgejobid),re.sub(r'^\s+',r'',x))]
                    if jobrecord:
                        jobrecordarray=jobrecord[0].split()
                        print(jobrecordarray[4])
                        if jobrecordarray[4] == 'qw':
                            pass
                        elif jobrecordarray[4] == 'Eqw':
                            self.killjob(sgejobid)
                            cplcode=1
                        elif jobrecordarray[4] == 'T':
                            self.killjob(sgejobid)
                            cplcode=1
                        elif jobrecordarray[4] == 'dr':
                            cplcode=1
                        elif jobrecordarray[4] == 't':
                            tcount+=1
                            if tcount > 5:
                                cplcode=1
                        elif jobrecordarray[4] == 'r':
                            qstatcmd="qstat -j %s" % sgejobid
                            stat=subprocess.Popen(qstatcmd,shell=True,stderr=subprocess.PIPE,stdout=subprocess.PIPE,universal_newlines=True)
                            returncode=-1
                            try:
                                returncode=stat.wait(timeout=120)
                            except:
                                pass
                            if returncode >=0:
                                stdout=stat.stdout.readlines()
                                stderr=stat.stderr.read()
                                if re.match(r'Following',stderr):
                                    cplcode=0
                                elif stderr:
                                    #some kind of sge error
                                    pass
                                else:
                                    usageAll=[x for x in stdout if re.match(r'usage',x)]
                                    for line in usageAll:
                                        aa=line.split()
                                        livevf=re.findall(r'vmem=\d+\.\d+.',line)
                                        if livevf:
                                            livevfs=livevf[0].replace('vmem=','')
                                            vmem=float(livevfs[0:-1])
                                            if livevfs[-1] == 'M':
                                                vmem/=1024
                                            if vmem >self.vf*1.5 or vmem >self.vf+5:
                                                try:
                                                    counttime[aa[1]]+=120
                                                except:
                                                    counttime[aa[1]]=120
                                            else:
                                                counttime[aa[1]]=0
                                            if counttime[aa[1]] >= 1200:
                                                newvf=vmem*1.5
                                                logging.info("jobid %s have break memory for 20min : set %d G used %d G; kill and reqsub new vf %s G" % (sgejobid,self.vf,vmem,newvf))
                                                self.vf=newvf
                                                cplcode=1
                                                break
                        else:
                            pass
                    else:
                        cplcode=0
            print("checkalive code %d" % (cplcode))
        return cplcode

    def checkcomplete(self,jobid,jobpart,jobname):
        if re.match(r'fail',jobid):
            return 1
        else:
            cplcode=-1
            uncpPart=[]
            while(cplcode<0):
                cmd="qstat -j %s" % jobid
                stat=subprocess.Popen(cmd,shell=True,stderr=subprocess.PIPE,stdout=subprocess.PIPE,universal_newlines=True)
                returncode=-1
                try:
                    returncode=stat.wait(timeout=120)
                except:
                    pass
                if returncode >=0:
                    stdout=stat.stdout.readlines()
                    stderr=stat.stderr.read()
                    if re.match(r'Following',stderr):
                        for part in jobpart:
                            shelldir="%s/shell/%s/%s.sh.o%s.%s" % (self.outdir,jobname,jobname,jobid,part)
                            shellerr="%s/shell/%s/%s.sh.e%s.%s" % (self.outdir,jobname,jobname,jobid,part)
                            try:
                                alllog=open(shelldir,mode='r').readlines()
                                if re.match(r'JobFinished',alllog[-1]):
                                    if cplcode<0:
                                        cplcode=0
                                else:
                                    uncpPart.append(part)
                                    cplcode=1
                            except:
                                uncpPart.append(part)
                                cplcode=1
                            try:
                                allerr=open(shellerr,mode='r').readlines()
                                for x in allerr:
                                    if re.search(r'Segmentation fault',x):
                                        cplcode=-1
                                        logging.info("Job %s with Jobid %s in fatal error;check %s for detail" % (jobname,jobid,shellerr))
                            except:
                                pass
                    elif re.match(r'========',stdout[0]):
                        pass
                    else:
                        pass
                print(cplcode)
                time.sleep(60)
            return cplcode,uncpPart

    def dumpjson(self,statedict,outputfile=None):
        self.output=self.input
        outputjson=outputfile
        if outputfile is None:
            outputjson=self.output
        try:
            oldjsondict=self.loadjson(outputjson)
            out=open(outputjson,mode='w')
            oldjsondict.update(statedict)                      
            json.dump(statedict,out,indent=4)#when step run parrallel, this file will be in wrong format. need to load the file first and then write the other in the same file
            out.close()
        except IOError as e:
            raise e
    
    def loadjson(self, inputfile=None):
        inputjson=inputfile
        if inputfile is None:
            inputjson=self.input
        try:
            jsondict=json.load(open(inputjson,mode='r'))
        except:
            jsondict={'control':'run'}
        return jsondict


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("use -h to check more parameter")
        sys.exit()
    parser=argparse.ArgumentParser(description="pipeline annotator help")
    parser.add_argument('--shellfile',dest="shellFile",type=str,help="the shell file would be submit")
    parser.add_argument('--line',dest="lineSplit",type=int,help="the line number contain in each split part. default split in 100 part. if you wish to run the whole shell in one job,set this to a number larger than the line number of the shell file and not equal 100")
    parser.add_argument('--vf',dest="vf",type=int,help="memory used in each job [ only support GB ]")
    parser.add_argument('--cpu',dest="cpu",type=int,help="cpu number set for each job")
    parser.add_argument('--queue',dest="queue",type=str,help="the queue use to run the job")
    parser.add_argument('--project',dest="project",type=str,help="the project code use to run the job. if the queue didn't need, don't set")
    parser.add_argument('--outdir',dest="outdir",type=str,help="the output dir used to output the log and job state, default current directory")

    localeArg=parser.parse_args()
    pwd=os.path.abspath('.')

    if localeArg.shellFile is None or localeArg.outdir is None or localeArg.queue is None:
        print("shellFile or outdir or queue not set")
        sys.exit()
    startw=jobexecutor()
    startw.outdir=localeArg.outdir
    try:
        command=open(localeArg.shellFile,mode='r').read()
        startw.command=command
    except IOError as e:
        raise e
    jobnamea=os.path.basename(localeArg.shellFile)
    jobname=re.sub(r'\..*',r'',jobnamea)
    if localeArg.vf is not None:
        startw.vf=localeArg.vf
    if localeArg.cpu is not None:
        startw.cpu=localeArg.cpu
    if localeArg.project is not None:
        startw.project=localeArg.project
    if localeArg.lineSplit is not None:
        startw.lineSplit=localeArg.lineSplit
    startw.runclusterjob(jobname=jobname)


