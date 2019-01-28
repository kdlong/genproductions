#!/usr/bin/python

'''
Script for POWHEG generator production
By Roberto Covarelli 10/30/2018
Based on Yuan Chao's script
'''

import commands
import fileinput
# import argparse
import sys
import os
from optparse import OptionParser
from Utilities import helpers

POWHEG_SOURCE = "powhegboxV2_rev3624_date20190117.tar.gz"
POWHEGRES_SOURCE = "powhegboxRES_rev3478_date20180122.tar.gz"
TESTING = 0
QUEUE = ''
rootfolder = os.getcwd()

def runCommand(command, printIt = False, doIt = 1, TESTING = 0) :
    if TESTING : 
        printIt = 1
        doIt = 0
    if printIt : print ('> ' + command)
    if doIt : 
        commandOutput = commands.getstatusoutput(command)
        if printIt : print commandOutput[1]
        return commandOutput[0]
    else :    print ('    jobs not submitted')
    return 1
    
# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

def prepareCondorScript( tag, i, folderName, queue, SCALE = '0' ):
   '''prepare the Condor submission script'''

   filename = 'run_' + tag + '.condorConf'
   execname = folderName + '/run_' + tag
   logname =  'run_' + tag
   f = open(filename, 'w')
  
   if (i == 'multiple') :
       f.write('executable              = ' + execname + '_$(ProcId).sh \n')
   elif (i == 'hnnlo') :
       f.write('executable              = ' + folderName + '/' + SCALE + '/' + 'launch_NNLO.sh \n')
       f.write('arguments               = HNNLO-LHC13-R04-APX2-' + SCALE + '.input $(ClusterId)$(ProcId) \n')
   else :
       f.write('executable              = ' + execname + '.sh \n')
   f.write('output                  = ' + logname + '_$(ProcId).out \n')
   f.write('error                   = ' + logname + '_$(ProcId).err \n')
   f.write('log                     = ' + logname + '.log \n')
   f.write('initialdir              = ' + rootfolder + '/' + folderName + '\n')

   f.write('+JobFlavour             = "'+ queue +'" \n') 

   f.write('periodic_remove         = JobStatus == 5  \n')
   f.write('WhenToTransferOutput    = ON_EXIT_OR_EVICT \n')
 
   f.write('\n')
 
   f.close()

   template_dict["additional_config"] = ""
   if os.path.exists('additional.condorConf') :
       input_file = open('additional.condorConf', 'r')
       template_dict["additional_config"] = "\n".join(extra_config.readlines())

   return filename


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

def prepareJob(tag, i, folderName) :
    filename = folderName+'/run_' + tag + '.sh'
    f = open(filename, 'w')

    f.write('#!/bin/bash \n')
    f.write('fail_exit() { echo "$@" 1>&2; exit 1; } \n\n')

    f.write('echo "Start of job on " `date`\n\n')

    f.write('cd '+os.getcwd()+'\n\n')

    f.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n\n')
    f.write('eval `scramv1 runtime -sh`\n\n')

    f.write('### Prepare environments for FastJet ### \n\n')

    f.write('export FASTJET_BASE=`scram tool info fastjet | grep FASTJET_BASE | sed -e s%FASTJET_BASE=%%`\n')
    f.write('export PATH=$FASTJET_BASE/bin/:$PATH \n')

    f.write('### Prepare environments for LHAPDF ### \n\n')
    
    f.write('LHAPDF6TOOLFILE=$CMSSW_BASE/config/toolbox/$SCRAM_ARCH/tools/available/lhapdf6.xml    \n')
    f.write('if [ -e $LHAPDF6TOOLFILE ]; then    \n')
    f.write('   export LHAPDF_BASE=`cat $LHAPDF6TOOLFILE | grep "<environment name=\\"LHAPDF6_BASE\\"" | cut -d \\" -f 4`    \n')
    f.write('else    \n')
    f.write('   export LHAPDF_BASE=`scram tool info lhapdf | grep LHAPDF_BASE | sed -e s%LHAPDF_BASE=%%`    \n')
    f.write('fi    \n')

    f.write('echo "LHAPDF_BASE is set to:" $LHAPDF_BASE \n')
    f.write('export PATH=$LHAPDF_BASE/bin/:$PATH \n')

    f.write('export LHAPDF_DATA_PATH=`$LHAPDF_BASE/bin/lhapdf-config --datadir` \n')

    f.write ('cd -' + '\n')
    f.write ('echo "I am here:"' + '\n')
    f.write ('pwd' + '\n')
    f.write ('cp -p ' + rootfolder + '/' + folderName + '/powheg.input ./' + '\n')
    f.write ('cp -p ' + rootfolder + '/' + folderName + '/JHUGen.input ./' + '\n')
    f.write ('cp -p ' + rootfolder + '/' + folderName + '/*.dat  ./' + '\n') 
    f.write ('cp -p ' + rootfolder + '/' + folderName + '/pwhg_main  ./' + '\n')
    f.write ('if [ -e '+ rootfolder + '/' + folderName + '/obj-gfortran/proclib ]; then    \n')
    f.write ('  mkdir ./obj-gfortran/' + '\n')
    f.write ('  cp -pr ' + rootfolder + '/' + folderName + '/obj-gfortran/proclib  ./obj-gfortran/' + '\n')
    f.write ('  cp -pr ' + rootfolder + '/' + folderName + '/obj-gfortran/*.so  ./obj-gfortran/' + '\n')
    f.write ('fi    \n')

    f.write('\n')

    f.close()
    return filename


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def prepareJobForEvents (tag, i, folderName, EOSfolder) :
    runCommand('rm ' + rootfolder + '/' + folderName + '/log_' + tag + '.log')
    filename = 'run_' + tag + '.sh'

    prepareJob(tag, i, folderName)

    f = open (filename, 'a')
    f.write ('cp -p ' + rootfolder + '/' + folderName + '/*.dat  ./' + '\n')
    f.write ('if [ -e '+ rootfolder + '/' + folderName + '/obj-gfortran/proclib ]; then    \n')
    f.write ('  mkdir ./obj-gfortran/' + '\n')
    f.write ('  cp -pr ' + rootfolder + '/' + folderName + '/obj-gfortran/proclib  ./obj-gfortran/' + '\n')
    f.write ('  cp -pr ' + rootfolder + '/' + folderName + '/obj-gfortran/*.so  ./obj-gfortran/' + '\n')
    f.write ('fi    \n')

    f.write ('cd -' + '\n')

    f.write ('pwd' + '\n')
    f.write ('ls' + '\n')
    f.write ('echo ' + str (i) + ' | ' + rootfolder + '/pwhg_main &> log_' + tag + '.log ' + '\n')
    f.write ('cp -p log_' + tag + '.log ' + rootfolder + '/' + folderName + '/. \n')
 
    f.close ()
    return filename


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def runParallelXgrid(parstage, xgrid, folderName, nEvents, njobs, powInputName, jobtag, rndSeed, process) :
    # parstage, xgrid are strings!

    print 'Running parallel jobs for grid'
    #print folderName

    inputName = folderName + "/powheg.input"

    sedcommand = 'sed -i "s/NEVENTS/'+nEvents+'/ ; s/SEED/'+rndSeed+'/ ; s/.*parallelstage.*/parallelstage '+parstage+'/ ; s/.*xgriditeration.*/xgriditeration '+xgrid+'/ ; s/.*manyseeds.*/manyseeds 1/ ; s/fakevirt.*// " '+inputName

    #print sedcommand
    runCommand(sedcommand)

    if (parstage == '1') :
        if not 'parallelstage' in open(inputName).read() :
            runCommand("echo \'\n\nparallelstage "+parstage+"\' >> "+inputName)
        if not 'xgriditeration' in open(inputName).read() :
            runCommand("echo \'xgriditeration "+xgrid+"\' >> "+inputName)

        if not 'manyseeds' in open(inputName).read() :
            runCommand("echo \'manyseeds 1\' >> "+ inputName)

        if not 'fakevirt' in open(inputName).read() :
            if process != 'b_bbar_4l':
                runCommand("echo \'fakevirt 1\' >> "+inputName)

    runCommand('cp -p '+inputName+' '+inputName+'.'+parstage+'_'+str(xgrid))

    for i in range (0, njobs) :
        jobID = jobtag + '_' + str(i)
        jobname = prepareJob(jobID, i, folderName)

        filename = folderName+'/run_' + jobID + '.sh'
        f = open(filename, 'a')
        #f.write('cd '+rootfolder+'/'+folderName+'/ \n')
        f.write('echo ' + str(i+1) + ' | ./pwhg_main &> run_' + jobID + '.log ' + '\n')
        f.write('cp -p *.top ' + rootfolder + '/' + folderName + '/. \n')
        f.write('cp -p *.dat ' + rootfolder + '/' + folderName + '/. \n')
        f.write('cp -p *.log ' + rootfolder + '/' + folderName + '/. \n')

        f.close()

        os.system('chmod 755 '+filename)

    if QUEUE == 'none':
        print 'Direct running... #'+str(i)+' \n'
        os.system('cd '+rootfolder+'/'+folderName)

        for i in range (0, njobs) :
            jobID = jobtag + '_' + str(i)
            os.system('bash run_'+jobID+'.sh &')

    else:
        print 'Submitting to condor queues:  \n'
        condorfile = prepareCondorScript(jobtag, 'multiple', args.folderName, QUEUE) 
        runCommand ('condor_submit ' + condorfile + ' -queue '+ str(njobs), TESTING == 0)


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
def runSingleXgrid(parstage, xgrid, folderName, nEvents, powInputName, seed, process, scriptName) :

    print 'Running single job for grid'
 
    inputName = folderName + "/powheg.input"

    sedcommand = 'sed "s/NEVENTS/' + nEvents + '/ ; s/SEED/' + seed + '/" ' + powInputName + ' > ' + folderName + '/powheg.input'

    runCommand(sedcommand)

    filename = scriptName

    f = open(filename, 'a')
    f.write('cd '+rootfolder+'/'+folderName+'/ \n')

    f.write('export LD_LIBRARY_PATH=`pwd`/lib/:`pwd`/lib64/:${LD_LIBRARY_PATH} \n\n')
 
    f.write('sed -i "s/NEVENTS/'+nEvents+'/ ; s/SEED/'+seed+'/" powheg.input\n\n')

    if process == 'gg_H_MSSM' :
        if os.path.exists(powInputName) :
            f.write('cp -p '+'/'.join(powInputName.split('/')[0:-1])+'/powheg-fh.in . \n')
        else :
            f.write('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+'/'.join(powInputName.split('/')[0:-1])+'/powheg-fh.in \n')

    if process == 'gg_H_2HDM' :
        if os.path.exists(powInputName) :
            f.write('cp -p '+'/'.join(powInputName.split('/')[0:-1])+'/br.a3_2HDM . \n')
            f.write('cp -p '+'/'.join(powInputName.split('/')[0:-1])+'/br.l3_2HDM . \n')
            f.write('cp -p '+'/'.join(powInputName.split('/')[0:-1])+'/br.h3_2HDM . \n')
        else :
            f.write('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+'/'.join(powInputName.split('/')[0:-1])+'/br.a3_2HDM \n')
            f.write('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+'/'.join(powInputName.split('/')[0:-1])+'/br.h3_2HDM \n')
            f.write('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+'/'.join(powInputName.split('/')[0:-1])+'/br.l3_2HDM \n')

    if process == 'VBF_HJJJ' :
        if os.path.exists(powInputName) :
            f.write('cp -p '+'/'.join(powInputName.split('/')[0:-1])+'/vbfnlo.input . \n')
        else :
            f.write('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+'/'.join(powInputName.split('/')[0:-1])+'/vbfnlo.input \n')

    m_ncall2 = 500000
    if process == 'ttH' :
        for line in open(inputName) :
            if 'ncall2' in line :
                m_ncall2 = line.split(" ")[2]
                print "The original ncall2 is :", m_ncall2

        f.write('sed -i "s/ncall2.*/ncall2 0/g" powheg.input \n')
        f.write('sed -i "s/fakevirt.*/fakevirt 1  ! number of calls for computing the integral and finding upper bound/g" powheg.input \n')

    f.write('./pwhg_main \n')

    if process == 'ttH' :
        f.write('sed -i "s/ncall2.*/ncall2 '+m_ncall2+'  ! number of calls for computing the integral and finding upper bound/g" powheg.input \n')
        f.write('sed -i "s/fakevirt.*/fakevirt 0/g" powheg.input \n')
        f.write('./pwhg_main \n')

    f.write('echo "\\nEnd of job on " `date` "\\n" \n')
    f.close()

    os.system('chmod 755 '+filename)


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
def runGetSource(parstage, xgrid, folderName, powInputName, process, noPdfCheck, tagName) :
    # parstage, xgrid are strings!

    print 'Getting and compiling POWHEG source...'

    filename = './run_%s.sh' % tagName
    template_dict = {
        "folderName" : folderName,
        "powInputName" : powInputName,
        "process" : process,
        "noPdfCheck" : noPdfCheck,
        "currDir" : os.getcwd(),
    }
    template_dict["rootfolder"] = rootfolder
    template_dict["script_dir"] = os.path.dirname(os.path.realpath(__file__))
    template_dict["patches_dir"] = os.path.dirname(os.path.realpath(__file__)) + "/patches"

    fourFlavorProcesses = ["ST_tch_4f", "bbH", "Wbb_dec", "Wbbj", "WWJ"]
    template_dict["isFiveFlavor"] = int(process not in fourFlavorProcesses)
    template_dict["defaultPDF"] = 306000 if template_dict["isFiveFlavor"] else 320900

    powhegResProcesses = ["b_bbar_4l", "HWJ_ew", "HW_ew", "HZJ_ew", "HZ_ew"] 
    if process in powhegResProcesses:
        template_dict["powhegSrc"] = POWHEGRES_SOURCE
    else:
        template_dict["powhegSrc"] = POWHEG_SOURCE

    template_file = "%s/Templates/compile_pwhg_template.sh" % template_dict["script_dir"]
    helpers.fillTemplatedFile(template_file, filename, template_dict)
    os.chmod(filename, 0o755)

    return

# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def runEvents(parstage, folderName, EOSfolder, njobs, powInputName, jobtag, process) :
    print 'run : submitting jobs'
  
    sedcommand = 'sed -i "s/parallelstage.*/parallelstage ' + parstage + '/ ; s/xgriditeration.*/xgriditeration 1/" '+folderName+'/powheg.input'

    runCommand(sedcommand)
    runCommand('cp -p ' + folderName + '/powheg.input ' + folderName + '/powheg.input.' + parstage)

    for i in range (0, njobs) :
        tag = jobtag + '_' + str (i)
        # real run
        if parstage == '4' : jobname = prepareJobForEvents(tag, i, folderName, EOSfolder)
        else               : jobname = prepareJob(tag, i, folderName)
        jobID = jobtag + '_' + str (i)
    
        filename = folderName+'/run_' + tag + '.sh'
        f = open (filename, 'a')
        f.write('cd '+rootfolder+'/'+folderName+'/ \n')
        f.write('echo ' + str (i) + ' | ./pwhg_main &> run_' + tag + '.log ' + '\n')
        f.close()

        os.system('chmod 755 '+filename)

    if QUEUE == 'none':
        print 'Direct running... #'+str(i)+' \n'
        os.system('cd '+rootfolder+'/'+folderName)
        
        for i in range (0, njobs) :
            jobID = jobtag + '_' + str(i)
            os.system('bash run_'+jobID+'.sh &')
            
    else:
        print 'Submitting to condor queues:  \n'
        condorfile = prepareCondorScript(jobtag, 'multiple', args.folderName, QUEUE) 
        runCommand ('condor_submit ' + condorfile + ' -queue '+ str(njobs), TESTING == 0)
     

# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

def createTarBall(parstage, folderName, prcName, keepTop, seed, scriptName) :
    print 'Creating tarball distribution for '+args.folderName+'_'+prcName+'.tgz'
    print

    #inputName = folderName + "/powheg.input"

    filename = scriptName

    f = open(filename, 'a')
    #if filename = "" :
    #    f = subprocess.Popen(['/bin/sh', '-c'])

    f.write('export folderName='+folderName+'\n\n')
    f.write('export process='+prcName+'\n\n')
#    f.write('export cardInput='+powInputName+'\n\n')
    f.write('export keepTop='+keepTop+'\n\n')
    f.write('export WORKDIR='+os.getcwd()+'\n\n')
    f.write('export SEED='+seed+'\n\n')
    f.write(
'''

cd $WORKDIR/$folderName
echo "Processing folder: "
pwd

rm -f $WORKDIR/$folderName'_'$process'.tgz'

cp -p $WORKDIR/run_pwg.py $WORKDIR/$folderName

if [ -e $WORKDIR/$folderName/pwggrid-0001.dat ]; then
  cp -p $WORKDIR/$folderName/pwggrid-0001.dat $WORKDIR/$folderName/pwggrid.dat
  cp -p $WORKDIR/$folderName/pwg-0001-stat.dat $WORKDIR/$folderName/pwg-stat.dat
fi


FULLGRIDRM=`ls ${WORKDIR}/${folderName} | grep fullgrid-rm`
FULLGRIDBTL=`ls ${WORKDIR}/${folderName} | grep fullgrid-btl`
if [ ${#FULLGRIDRM} -gt 0 -a ${#FULLGRIDBTL} -gt 0 ]; then
  cp -p $WORKDIR/$folderName/${FULLGRIDRM} $WORKDIR/$folderName/pwgfullgrid-rm.dat
  cp -p $WORKDIR/$folderName/${FULLGRIDBTL} $WORKDIR/$folderName/pwgfullgrid-btl.dat
  cp -p $WORKDIR/$folderName/pwg-0001-st3-stat.dat $WORKDIR/$folderName/pwg-stat.dat
fi

grep -q "NEVENTS" powheg.input; test $? -eq 0 || sed -i "s/^numevts.*/numevts NEVENTS/g" powheg.input
grep -q "SEED" powheg.input; test $? -eq 0 || sed -i "s/^iseed.*/iseed SEED/g" powheg.input

grep -q "manyseeds" powheg.input; test $? -eq 0 || printf "\\n\\nmanyseeds 1\\n" >> powheg.input
grep -q "parallelstage" powheg.input; test $? -eq 0 || printf "\\nparallelstage 4\\n" >> powheg.input
grep -q "xgriditeration" powheg.input; test $? -eq 0 || printf "\\nxgriditeration 1\\n" >> powheg.input
  
# turn into single run mode
sed -i "s/^manyseeds.*/#manyseeds 1/g" powheg.input
sed -i "s/^parallelstage.*/#parallelstage 4/g" powheg.input
sed -i "s/^xgriditeration/#xgriditeration 1/g" powheg.input

# turn off obsolete stuff
grep -q "pdfreweight" powheg.input; test $? -eq 0 || printf "\\n\\npdfreweight 0\\n" >> powheg.input
grep -q "storeinfo_rwgt" powheg.input; test $? -eq 0 || printf "\\nstoreinfo_rwgt 0\\n" >> powheg.input
grep -q "withnegweights" powheg.input; test $? -eq 0 || printf "\\nwithnegweights 1\\n" >> powheg.input
  
sed -i "s/^pdfreweight.*/#pdfreweight 0/g" powheg.input
sed -i "s/^storeinfo_rwgt.*/#storeinfo_rwgt 0/g" powheg.input
sed -i "s/^withnegweights/#withnegweights 1/g" powheg.input

# parallel re-weighting calculation
if [ "$process" = "HW_ew" ] || [ "$process" = "HZ_ew" ] || [ "$process" = "HZJ_ew" ] || [ "$process" = "HWJ_ew" ] ; then
   echo "# no reweighting in first runx" >> powheg.input
else 
   echo "rwl_group_events 2000" >> powheg.input
   echo "lhapdf6maxsets 50" >> powheg.input
   echo "rwl_file 'pwg-rwl.dat'" >> powheg.input
   echo "rwl_format_rwgt 1" >> powheg.input
fi
cp -p $WORKDIR/pwg-rwl.dat pwg-rwl.dat

if [ -e ${WORKDIR}/$folderName/cteq6m ]; then
    cp -p ${WORKDIR}/cteq6m .
fi

if [ -s ${WORKDIR}/$folderName/JHUGen.input ]; then
    sed -e "s/PROCESS/${process}/g" ${WORKDIR}/runcmsgrid_powhegjhugen.sh > runcmsgrid.sh
else
    sed -e "s/PROCESS/${process}/g" ${WORKDIR}/runcmsgrid_powheg.sh > runcmsgrid.sh
fi

sed -i 's/pwggrid.dat ]]/pwggrid.dat ]] || [ -e ${WORKDIR}\/pwggrid-0001.dat ]/g' runcmsgrid.sh

if [ "$process" = "WWJ" ]; then
   cp -p ${WORKDIR}/$folderName/POWHEG-BOX/$process/testrun-nnlops/binvalues-WW.top .
   cp -r ${WORKDIR}/$folderName/POWHEG-BOX/$process/testrun-nnlops/WW_MATRIX .
   cp -r ${WORKDIR}/$folderName/POWHEG-BOX/$process/testrun-nnlops/WW_MINLO .
   keepTop='1'
fi  

sed -i s/SCRAM_ARCH_VERSION_REPLACE/${SCRAM_ARCH}/g runcmsgrid.sh
sed -i s/CMSSW_VERSION_REPLACE/${CMSSW_VERSION}/g runcmsgrid.sh

sed -i s/SCRAM_ARCH_VERSION_REPLACE/${SCRAM_ARCH}/g runcmsgrid.sh
sed -i s/CMSSW_VERSION_REPLACE/${CMSSW_VERSION}/g runcmsgrid.sh
chmod 755 runcmsgrid.sh
cp -p runcmsgrid.sh runcmsgrid_par.sh

sed -i '/ reweightlog_/c cat <<EOF | ../pwhg_main &>> reweightlog_${process}_${seed}.txt\\n${seed}\\npwgevents.lhe\\nEOF\\n' runcmsgrid_par.sh
sed -i 's/# Check if /sed -i \"s#.*manyseeds.*#manyseeds 1#g\" powheg.input\\n# Check if /g' runcmsgrid_par.sh
sed -i 's/# Check if /sed -i \"s#.*parallelstage.*#parallelstage 4#g\" powheg.input\\n# Check if /g' runcmsgrid_par.sh
sed -i 's/# Check if /sed -i \"s#.*xgriditeration.*#xgriditeration 1#g\" powheg.input\\n\\n# Check if /g' runcmsgrid_par.sh
sed -i 's/# Check if /rm -rf pwgseeds.dat; for ii in $(seq 1 9999); do echo $ii >> pwgseeds.dat; done\\n\\n# Check if /g' runcmsgrid_par.sh
sed -i 's/^..\/pwhg_main/echo \${seed} | ..\/pwhg_main/g' runcmsgrid_par.sh
sed -i 's/\.lhe/\${idx}.lhe/g' runcmsgrid_par.sh
sed -i 's/pwgevents.lhe/fornnlops/g' nnlopsreweighter.input
sed -i "s/^process/idx=-\`echo \${seed} | awk \'{printf \\"%04d\\", \$1}\'\` \\nprocess/g" runcmsgrid_par.sh

chmod 755 runcmsgrid_par.sh

#cd ${WORKDIR}

if [ "$process" = "HJ" ]; then
  echo "This process needs NNLOPS reweighting"
  for i in `echo 11 22 0505`; do
    ./mergedata 1 ${i}/*.top
    mv fort.12 HNNLO-${i}.top 
  done
  #force keep top in this case 
  keepTop='1'
fi

if [ $keepTop == '1' ]; then
    echo 'Keeping validation plots.'
    echo 'Packing...' ${WORKDIR}'/'${process}'_'${SCRAM_ARCH}'_'${CMSSW_VERSION}'_'${folderName}'.tgz'
    tar zcf ${WORKDIR}'/'${process}'_'${SCRAM_ARCH}'_'${CMSSW_VERSION}'_'${folderName}'.tgz' * --exclude=POWHEG-BOX --exclude=powhegbox*.tar.gz --exclude=*.lhe --exclude=run_*.sh --exclude=*temp --exclude=pwgbtlupb-*.dat --exclude=pwgrmupb-*.dat
else
    echo 'Packing...' ${WORKDIR}'/'${process}'_'${SCRAM_ARCH}'_'${CMSSW_VERSION}'_'${folderName}'.tgz'
    tar zcf ${WORKDIR}'/'${process}'_'${SCRAM_ARCH}'_'${CMSSW_VERSION}'_'${folderName}'.tgz' * --exclude=POWHEG-BOX --exclude=powhegbox*.tar.gz --exclude=*.top --exclude=*.lhe --exclude=run_*.sh --exclude=*temp --exclude=pwgbtlupb-*.dat --exclude=pwgrmupb-*.dat
fi

cd ${WORKDIR}

date
echo 'Done.'

''')

    f.close()

    os.system('chmod 755 '+filename)

# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def runhnnlo(folderName, njobs, QUEUE):
  scales = ["11", "22", "0505"]
  for scale in scales:
    os.system('rm -rf '+ folderName+"/"+scale)
    os.system('mkdir -p '+ folderName+"/"+scale) 
    filename = folderName+"/"+scale+"/launch_NNLO.sh"
    launching_script = open(filename, "w")
    launching_script.write("#!/bin/bash\n")
    launching_script.write('base='+os.getcwd()+"/"+folderName+"/"+scale+'\n\n')
    launching_script.write(
'''
config=$1
seed=$2

cd $base
eval `scram runtime -sh`
cd -

cat $base/../$config | sed -e "s#SEED#$seed#g" > config.input
cat config.input | sed -e "s#MSTW2008nnlo68cl#NNPDF31_nnlo_hessian_pdfas#g" > config.input.temp
mv config.input.temp config.input

cp $base/../hnnlo .
cp $base/../br* .

./hnnlo < config.input &> log_${seed}.txt

cp HNNLO-LHC13* ${base}

cp log_${seed}.txt ${base}
''')
    launching_script.close()
    os.system('chmod 755 '+filename) 
     
    print 'Submitting to condor queues \n'
    tagName = 'hnnlo_' + scale 
    condorfile = prepareCondorScript(tagName, 'hnnlo', folderName, QUEUE, scale) 
    runCommand ('condor_submit ' + condorfile + ' -queue '+ str(njobs), TESTING == 0)
   


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


if __name__ == "__main__":

    eoscmd = '/afs/cern.ch/project/eos/installation/cms/bin/eos.select' ;

    parser = OptionParser()
    parser.add_option('-p', '--parstage'      , dest="parstage",      default= '0',            help='stage of the production process [0]')
    parser.add_option('-x', '--xgrid'         , dest="xgrid",         default= '1',            help='loop number for the grids production [1]')
    parser.add_option('-f', '--folderName'    , dest="folderName",    default='testProd',      help='local folder and last eos folder name[testProd]')
    parser.add_option('-e', '--eosFolder'     , dest="eosFolder",     default='NONE' ,         help='folder before the last one, on EOS')
    parser.add_option('-j', '--numJobs'       , dest="numJobs",       default= '10',           help='number of jobs to be used for multicore grid step 1,2,3')
    parser.add_option('-t', '--totEvents'     , dest="totEvents",     default= '10000',        help='total number of events to be generated [10000]')
    parser.add_option('-n', '--numEvents'     , dest="numEvents",     default= '2000',         help='number of events for a single job [2000]')
    parser.add_option('-i', '--inputTemplate' , dest="inputTemplate", default= 'powheg.input', help='input cfg file (fixed) [=powheg.input]')
    parser.add_option('-g', '--inputJHUGen' , dest="inputJHUGen", default= '', help='input JHUGen cfg file []')
    parser.add_option('-q', '--doQueue'      , dest="doQueue",      default= 'none',          help='Jobflavour: if none, running interactively [none]')
    parser.add_option('-s', '--rndSeed'       , dest="rndSeed",       default= '42',           help='Starting random number seed [42]')
    parser.add_option('-m', '--prcName'       , dest="prcName",       default= 'DMGG',           help='POWHEG process name [DMGG]')
    parser.add_option('-k', '--keepTop'       , dest="keepTop",       default= '0',           help='Keep the validation top draw plots [0]')
    parser.add_option('-d', '--noPdfCheck'    , dest="noPdfCheck",    default= '0',           help='If 1, deactivate automatic PDF check [0]')

    # args = parser.parse_args ()
    (args, opts) = parser.parse_args(sys.argv)
    
    QUEUE = args.doQueue
    EOSfolder = args.folderName

    print
    print 'RUNNING PARAMS: parstage = ' + args.parstage + ' , xgrid = ' + args.xgrid  + ' , folderName = ' + args.folderName 
    print '                Total Events = ' + args.totEvents 
    print '                Number of Events = ' + args.numEvents 
    print '                powheg input cfg file : ' + args.inputTemplate 
    print '                powheg process name : ' + args.prcName
    print '                working folder : ' + args.folderName
 #   print '                EOS folder : ' + args.eosFolder + '/' + EOSfolder
    print '                base folder : ' + rootfolder
    print
 
    if (TESTING == 1) :     
        print '  --- TESTING, NO submissions will happen ---  '
        print

    res = os.path.exists(rootfolder+'/'+args.folderName)

    if args.parstage == '1' and args.xgrid == '1' and (not res) :
        print 'Creating working folder ' + args.folderName + '...'
        # Assuming the generator binaries are in the current folder.
        os.system('mkdir '+rootfolder+'/'+args.folderName)
        if os.path.exists(rootfolder+'/pwhg_main') :
            print 'Copy pwhg_main'
            os.system('cp -p pwhg_main '+args.folderName+'/.')

        if os.path.exists(rootfolder+'JHUGen') :
            print 'Copy JHUGen'
            os.system('cp -p JHUGen '+args.folderName+'/.')

    if args.parstage == '1' and args.xgrid == '1' :
        if not os.path.exists(args.folderName) :
            print 'Creating working folder ' + args.folderName + '...'
            # Assuming the generator binaries are in the current folder.
            os.system('mkdir '+args.folderName)
            if os.path.exists('pwhg_main') :
                os.system('cp -p pwhg_main '+args.folderName+'/.')

            if os.path.exists('JHUGen') :
                os.system('cp -p JHUGen '+args.folderName+'/.')

        if not os.path.exists(args.inputTemplate) :
            os.system('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+args.inputTemplate+' -O '+args.folderName+'/powheg.input')
            os.system('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+args.inputTemplate)

            os.system('sed -i "s/^numevts.*/numevts '+args.numEvents+'/" '+
                      args.folderName+'/powheg.input')

        if not os.path.exists(args.folderName+'/powheg.input') :
            os.system('cp -p '+args.inputTemplate+' '+
                      args.folderName+'/powheg.input')
            os.system('sed -i "s/^numevts.*/numevts '+args.numEvents+'/" '+
                      args.folderName+'/powheg.input')

        if not os.path.exists(args.folderName + '/pwgseeds.dat') :
            fseed = open(args.folderName + '/pwgseeds.dat', 'w')
            for ii in range(1, 10000) :
                fseed.write(str(ii)+'\n')
            fseed.close()

    if args.parstage == '4' :    
        runCommand (eoscmd + ' mkdir /eos/cms/store/user/${user}/LHE/powheg/' + args.eosFolder, 1, 1)
        runCommand (eoscmd + ' mkdir /eos/cms/store/user/${user}/LHE/powheg/' + args.eosFolder + '/' + EOSfolder, 1, 1)

    njobs = int (args.numJobs)

    powInputName = args.inputTemplate
    jobtag = args.parstage + '_' + args.xgrid

    if len(sys.argv) <= 1 :
        print "\t argument '-p', '--parstage'      , default= '0'"
        print "\t argument '-x', '--xgrid'         , default= '1'"
        print "\t argument '-f', '--folderName'    , default='testProd'"
        print "\t argument '-e', '--eosFolder'     , default='NONE'"
        print "\t argument '-t', '--totEvents'     , default= '10000"
        print "\t argument '-n', '--numEvents'     , default= '2000'"
        print "\t argument '-i', '--inputTemplate' , default= 'powheg.input'"
        print "\t argument '-g', '--inputJHUGen'   , default= ''"
        print "\t argument '-q', '--doQueue'       , default= ''"
        print "\t argument '-s', '--rndSeed'       , default= '42'"
        print "\t argument '-m', '--prcName'       , default= 'DMGG'"
        print "\t argument '-k', '--keepTop'       , default= '0'"
        print "\t argument '-d', '--noPdfCheck'    , default= '0'"
        print ""

        exit()

    if args.parstage == '0' or \
       args.parstage == '0123' or args.parstage == 'a' or \
       args.parstage == '01239' or args.parstage == 'f' : # full single grid in oneshot 

        tagName = 'src_'+args.folderName
        filename = './run_'+tagName+'.sh'

        prepareJob(tagName, '', '.')

        if not os.path.exists(args.inputTemplate) :
            m_ret = os.system('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+args.inputTemplate+' -O '+args.folderName+'/powheg.input')

            os.system('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+args.inputTemplate)
        
        os.system('mkdir -p '+rootfolder+'/'+args.folderName)
        if not os.path.exists(args.inputTemplate) :
            os.system('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+args.inputTemplate+' -O '+args.folderName+'/powheg.input')
        else :
            os.system('cp -p '+args.inputTemplate+' '+args.folderName+'/powheg.input')

        os.system('rm -rf JHUGen.input')
        inputJHUGen = args.inputJHUGen
        if args.inputJHUGen == "":
            inputJHUGen = '/'.join(powInputName.split('/')[0:-1])+'/JHUGen.input'

        if not os.path.exists(inputJHUGen) :
            m_ret = os.system('wget --quiet --no-check-certificate -N http://cms-project-generators.web.cern.ch/cms-project-generators/'+inputJHUGen+' -O '+args.folderName+'/JHUGen.input')
            if ((m_ret>>8) & 255) != 0 :
                os.system('rm -rf '+args.folderName+'/JHUGen.input')

        else :
            os.system('cp -p '+inputJHUGen+' '+args.folderName+'/JHUGen.input')

        if os.path.exists(args.folderName+'/powheg.input') :
            test_pdf1 = 0
            test_pdf2 = 0

            default_pdf = "306000"  # for 5 flavours

            if args.prcName=="ST_tch_4f" or args.prcName=="bbH" or args.prcName=="Wbb_dec" or args.prcName=="Wbbj" or args.prcName=="WWJ" :
                default_pdf = "320900"  # for 4 flavours

            for line in open(args.folderName+'/powheg.input') :
                n_column = line.split()
                if 'lhans1' in line and len(n_column) >= 2:
                    test_pdf1 = n_column[1].strip()
                if 'lhans2' in line and len(n_column) >= 2:
                    test_pdf2 = n_column[1].strip()

            if not (test_pdf1 == test_pdf2) :
                raise RuntimeError("ERROR: PDF settings not equal for the 2 protons: {0} vs {1}... Please check your datacard".format(test_pdf1, test_pdf2))

            if test_pdf1 != default_pdf :
#                print "PDF in card: ", test_pdf1, "PDF default: ", default_pdf, test_pdf1==default_pdf
                message = "The input card does not have the standard 2017 PDF (NNPDF31 NNLO, 306000 for 5F, 320900 for 4F): {0}. Either change the card or run again with -d 1 to ignore this message.\n".format(test_pdf1)

                if args.noPdfCheck == '0' :
                    raise RuntimeError(message)
                else:
                    print "WARNING:", message
                    print "FORCING A DIFFERENT PDF SET FOR CENTRAL VALUE\n"

    if args.parstage == '0' :

        tagName = 'src_'+args.folderName
        filename = './run_'+tagName+'.sh'

        prepareJob(tagName, '', '.')

        runGetSource(args.parstage, args.xgrid, args.folderName,
                     powInputName, args.prcName, args.noPdfCheck, tagName)

        if QUEUE == 'none':
            print 'Direct compiling... \n'
            os.system('bash '+filename+' 2>&1 | tee '+filename.split('.sh')[0]+'.log')
        
        else:
            print 'Submitting to condor queues \n'
            condorfile = prepareCondorScript(tagName, '', '.', QUEUE) 
            runCommand ('condor_submit ' + condorfile + ' -queue 1', TESTING == 0)

    elif args.parstage == '1' :
        runParallelXgrid(args.parstage, args.xgrid, args.folderName,
                         args.numEvents, njobs, powInputName, jobtag,
                         args.rndSeed, args.prcName)

    elif args.parstage == '123' or args.parstage == 's' : # single grid proc
        tagName = 'grid_'+args.folderName
        scriptName = args.folderName + '/run_'+tagName+'.sh'


        os.system('cp -p '+args.inputTemplate+' '+args.folderName+'/powheg.input')
        os.system('sed -i "s/^numevts.*/numevts '+args.totEvents+'/" '+
                  args.folderName+'/powheg.input')

        prepareJob(tagName, '', args.folderName)
        runSingleXgrid(args.parstage, args.xgrid, args.folderName,
                       args.numEvents, powInputName, args.rndSeed,
                       args.prcName, scriptName)

        if QUEUE == 'none':
            print 'Direct running single grid... \n'
            os.system('bash '+scriptName+' >& '+scriptName.split('.sh')[0]+'.log &')
                    
        else:
            print 'Submitting to condor queues  \n'
            condorfile = prepareCondorScript(tagName, '', args.folderName, QUEUE) 
            runCommand ('condor_submit ' + condorfile + ' -queue 1', TESTING == 0)

    elif args.parstage == '0123' or args.parstage == 'a' : # compile & run
        tagName = 'all_'+args.folderName
        scriptName = './run_'+tagName+'.sh'

        prepareJob(tagName, '', '.')
        runGetSource(args.parstage, args.xgrid, args.folderName,
                     powInputName, args.prcName, args.noPdfCheck, tagName)

        os.system('sed -i "s/^numevts.*/numevts '+args.numEvents+'/" '+
                  args.folderName+'/powheg.input')
        runSingleXgrid(args.parstage, args.xgrid, args.folderName,
                       args.numEvents, powInputName, args.rndSeed,
                       args.prcName, scriptName)

        if QUEUE == '':
            print 'Direct compiling and running... \n'
            #runCommand ('bash run_source.sh ', TESTING == 1)
            os.system('bash '+scriptName+' >& '+
                      scriptName.split('.sh')[0]+'.log &')
        else:
            print 'Submitting to condor queues  \n'
            condorfile = prepareCondorScript(tagName, '', '.', QUEUE) 
            runCommand ('condor_submit ' + condorfile + ' -queue 1', TESTING == 0)

    elif args.parstage == '01239' or args.parstage == 'f' : # full single grid in oneshot 
        tagName = 'full_'+args.folderName
        scriptName = './run_'+tagName+'.sh'

        prepareJob(tagName, '', '.')
        runGetSource(args.parstage, args.xgrid, args.folderName,
                     powInputName, args.prcName, args.noPdfCheck, tagName)

        runSingleXgrid(args.parstage, args.xgrid, args.folderName,
                       args.numEvents, powInputName, args.rndSeed,
                       args.prcName, scriptName)

        createTarBall(args.parstage, args.folderName, args.prcName,
                      args.keepTop, args.rndSeed, scriptName)

        if QUEUE == '':
            print 'Direct running in one shot... \n'
            os.system('bash '+scriptName+' >& '+
                      scriptName.split('.sh')[0]+'.log &')
        else:
            print 'Submitting to condor queues  \n'
            condorfile = prepareCondorScript(tagName, '', '.', QUEUE) 
            runCommand ('condor_submit ' + condorfile + ' -queue 1', TESTING == 0)

    elif args.parstage == '7' :
      print "preparing for NNLO reweighting"
      runhnnlo(args.folderName, njobs, QUEUE)

    elif args.parstage == '9' :
        # overwriting with original
        scriptName = './run_tar_'+args.folderName+'.sh'

        os.system('rm -rf '+scriptName)

        createTarBall(args.parstage, args.folderName, args.prcName,
                      args.keepTop, args.rndSeed, scriptName)

        os.system('cd '+rootfolder+';bash '+scriptName)

    else                    :
        runEvents(args.parstage, args.folderName,
                  args.eosFolder + '/' + EOSfolder, njobs, powInputName,
                  jobtag, args.prcName)
