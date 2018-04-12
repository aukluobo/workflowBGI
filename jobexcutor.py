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
        self.input="%s/state/state.json" % (self.outdir)
        self.output=self.input
        self.vf=1
        self.cpu=1
        self.queue="st.q"
        self.project=None
    
    def runclusterjob(self,commandshell=None,jobname=None):
        takecommand=commandshell
        usedjobname=jobname
        os.makedirs("%s/state" % (self.outdir),mode=0o755,exist_ok=True)
        if commandshell is None:
            takecommand=self.command
        if jobname is None:
            usedjobname="test"
        statedict=self.loadjson()
        if statedict['control'] == 'run':
            globalcode=1
            while(globalcode > 0):
                recode,submitid=self.submitjob(takecommand,usedjobname)
                timecap=30
                if recode == 0:
                    statedict[usedjobname]=submitid
                elif recode > 0:
                    resubmitid=self.rerunjob(recode,timecap,takecommand,usedjobname)
                    statedict[usedjobname]=resubmitid
                else:
                    globalcode=-1
                    statedict[usedjobname]=submitid

                self.dumpjson(statedict)
                time.sleep(30)
                if globalcode > 0:
                    alivecode=self.checkalive(statedict[usedjobname])
                    if alivecode==0:
                        globalcode=self.checkcomplete(statedict[usedjobname],usedjobname)
            if globalcode==0:
                statedict[usedjobname]='completed'
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
        out.write(commandshell+"\n")
        out.write("echo JobFinished")
        out.close()
        qsubcommand="qsub -terse -N %s.sh -wd %s -l vf=%sG,num_proc=%d -q %s " % (jobname,shelldir,self.vf,self.cpu,self.queue)
        if self.project is not None:
            qsubcommand+="-P %s" % (self.project)
        qsubcommand+=" %s/%s.sh" % (shelldir,jobname)
        logging.info(qsubcommand)
        submit=subprocess.Popen(qsubcommand,shell=True,stderr=subprocess.PIPE,stdout=subprocess.PIPE,universal_newlines=True)
        returncode=submit.wait(timeout=120)
        if returncode == 0:
            stdout=submit.stdout.read()
            stderr=submit.stderr.read()
            print(stdout+"\n"+stderr)
            if stderr is not '':
                return -1,stderr
            else:
                return returncode,stdout.replace('\n','')
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
        cmd='qdel %s' % (sgejobid)
        subprocess.Popen(cmd,shell=True,universal_newlines=True)


    def hodljob(self):
        pass
    def checkalive(self, sgejobid):
        cplcode=-1
        counttime=0
        tcount=0
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
                    jobrecord=[x for x in stdoutall if re.match(re.escape(sgejobid),x)]
                    if jobrecord:
                        jobrecordarray=jobrecord[0].split()
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
                                else:
                                    livevf=re.findall(r'veme=\d+',[x for x in stdout if re.match(r'usage',x)][0])
                                    if livevf >self.vf*1.5 or livevf >self.vf+5:
                                        counttime+=120
                                    else:
                                        counttime=0
                                    if counttime > 1200:
                                        newvf=livevf*1.5
                                        logging.info("jobid %s have break memory for 20min : set %d G used %d G; kill and reqsub new vf %s G" % (sgejobid,self.vf,livevf,newvf))
                                        self.vf=newvf
                                        cplcode=1
                        else:
                            pass


    def checkcomplete(self, jobid,jobname):
        if re.match(r'fail',jobid):
            return 1
        else:
            cplcode=-1
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
                        shelldir="%s/shell/%s/%s.sh.o%s" % (self.outdir,jobname,jobname,jobid)
                        alllog=open(shelldir,mode='r').readlines()
                        if re.match(r'JobFinished',alllog[-1]):
                            cplcode=0
                        else:
                            cplcode=1
                    elif re.match(r'========',stdout[0]):
                        pass
                    else:
                        pass
                print(cplcode)
                time.sleep(60)
            return cplcode

    def dumpjson(self,statedict,outputfile=None):
        outputjson=outputfile
        if outputfile is None:
            outputjson=self.output
        try:
            out=open(outputjson,mode='w')                      
            json.dump(statedict,out)
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
