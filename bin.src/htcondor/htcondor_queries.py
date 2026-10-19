#!/usr/bin/env python

import time
import datetime
import json
import os
import classad
import htcondor2 as htcondor
from opensearchpy import OpenSearch

def configure_htcondor():

    htcondor.param['SEC_CLIENT_AUTHENTICATION_METHODS'] = 'SSL FS IDTOKENS FS_REMOTE'
    htcondor.param['SEC_CLIENT_AUTHENTICATION'] = 'OPTIONAL'
    htcondor.param['SEC_CLIENT_INTEGRITY'] = 'OPTIONAL'
    htcondor.param['SEC_CLIENT_ENCRYPTION'] = 'OPTIONAL'


# sampling interval in seconds
sample_interval = 15
# max run time in seconds. So sample every sample_interval seconds up to max_run.
max_run = 60
debug = False
host = "usdf-opensearch.slac.stanford.edu"
port = 443
auth = ("condor", os.environ['OSPWD'])
client = OpenSearch(hosts = [{'host': host, "port": port}],http_compress= True,
http_auth=auth, use_ssl = True, verify_certs = True, ssl_assert_hostname= False,
ssl_show_warn = False)

os_index_name = 'htcondor-dev-totals-v2'

# define a "standard" memory per core value
memory_slot = 4096.0

# for localhost to work unauthenticated, you must run on
# the machine where the collector is located (usually sdfiana012).
mycoll = ['localhost:9618', '134.79.23.197:9618']

configure_htcondor()
scheddads = None

for icoll in mycoll:
    coll = htcondor.Collector(icoll)
    if scheddads is None:
        scheddads = coll.locateAll(htcondor.DaemonTypes.Schedd)
    else:
        scheddads += coll.locateAll(htcondor.DaemonTypes.Schedd)

run_constraint = 'JobStatus==2&&JobUniverse!=7'
idle_constraint = 'JobStatus==1&&JobUniverse!=7'
dagman_constraint = 'JobUniverse!=7'

stats = ["ClusterId","ProcId","JobStatus","JobUniverse",
               "QDate","ServerTime","JobCurrentStartDate","RemoteUserCpu",
               "EnteredCurrentStatus", "MachineAttrGLIDEIN_Site0"]
extra_stats=[
    "Owner","AccountingGroup", "CpusProvisioned","NumRestarts",
    "NumJobStarts","RemoteUserCpu", "RequestMemory",
    "RequestDisk", "RequestCpus", "ResidentSetSize_RAW",
    "DiskUsage_RAW", "HoldReason", "HoldReasonCode",
    "HoldReasonSubcode", "bps_job_name", "bps_job_label",
    "bps_campaign", "bps_job_quanta", "bps_job_summary",
    "bps_operator", "bps_payload", "bps_project",
    "bps_run", "bps_run_quanta", "bps_runsite",
    "JobNodeset", "StartdName", "GridResource", "RemoteHost"
    ]


start_time = time.time()
jobs = []
dictlist = []
nsamples = 0
totals = {}
totals['timestamp'] = int(start_time)

jobkeys = ["Idle", "Running", "Held", "TotalCpus", "TotalRemoteUserCpu",
           "TotalWallTime", "WeightedCpus", "Efficiency"]           

tprev = start_time
while time.time() < start_time + max_run:
    tnow = time.time()
    # if we don't have enough time to run again before max_run, bail out
    if tnow + sample_interval > start_time + max_run:
        break
    # if it isn't time to sample again yet, wait until it is
    if tnow < tprev + sample_interval and nsamples>0:
        time.sleep(tprev + sample_interval - tnow)
    tprev = time.time()

    for ischedd in scheddads:
        schedd = htcondor.Schedd(ischedd)
        scheddname = ischedd.get("name")
        jobs = []
        try:
            jobs = schedd.query(dagman_constraint,
                            stats+extra_stats)
        except Exception as e:
            failtime = datetime.datetime.fromtimestamp(time.time())
            print(f"Failure to query {scheddname} at {failtime}:")
            print(str(e))
        # assume we're OK with 1-second accuracy
        sitename = ""
        sitefromschedd = False
        if "sdf.slac.stanford.edu" in scheddname:
            sitename = "SLAC"
            sitefromschedd = True
        # add stuff for other sites here down the road
        else:
            sitename = "UNKNOWN"
        for ijob in jobs:
            #convert to json
            jobjson = json.loads(ijob.printJson())
            if not sitefromschedd:
                # attempt to determine run site based on classad info
                if 'MachineAttrGLIDEIN_Site0' in jobjson.keys():
                    sitename = jobjson['MachineAttrGLIDEIN_Site0'].upper()
                elif 'GridResource' in jobjson.keys():
                    if "gridpp.rl.ac.uk" in jobjson['GridResource']:
                        sitename = "RAL"
                    elif "lancs.ac.uk" in jobjson['GridResource']:
                        sitename = "LANCS"
                    elif "in2p3.fr" in jobjson['GridResource']:
                        sitename = "IN2P3"
                elif 'StartdName' in jobjson.keys():
                    if "slac.stanford.edu" in jobjson['StartdName']:
                        sitename = "SLAC"
                    elif "in2p3.fr" in jobjson['StartdName']:
                        sitename = "IN2P3"
                    elif "gridpp.rl" in jobjson['StartdName']:
                        sitename = "RAL"
                    elif "private.dns.zone" in jobjson['StartdName']:
                        sitename = "LANCS"
                elif 'bps_runsite' in jobjson.keys():
                    sitename = jobjson['bps_runsite'].upper()
                elif 'JobNodeset' in jobjson.keys():
                    sitename = jobjson['JobNodeset'].upper()

            if sitename not in totals.keys():
                totals[sitename] = {}
            if jobjson['Owner'] not in totals[sitename].keys():
                totals[sitename][jobjson['Owner']] = {}
            if 'bps_run' not in jobjson.keys():
                jobjson['bps_run'] = "undefined"
            if 'bps_job_label' not in jobjson.keys():
                jobjson['bps_job_label'] = "undefined"
            if jobjson['bps_run'] not in totals[sitename][jobjson['Owner']].keys():
                totals[sitename][jobjson['Owner']][jobjson['bps_run']] = {}
            if jobjson['bps_job_label'] not in totals[sitename][jobjson['Owner']][jobjson['bps_run']].keys():
                totals[sitename][jobjson['Owner']][jobjson['bps_run']][jobjson['bps_job_label']] = {}
                activedict = totals[sitename][jobjson['Owner']][jobjson['bps_run']][jobjson['bps_job_label']]
                for jkey in jobkeys:
                    activedict[jkey] = 0
            activedict = totals[sitename][jobjson['Owner']][jobjson['bps_run']][jobjson['bps_job_label']]  
            
            if jobjson['JobStatus'] == 1:
                activedict['Idle'] += 1
            elif jobjson['JobStatus'] == 2:
                activedict['Running'] += 1
                ncpu = 1
                if "CpusProvisioned" in jobjson.keys():
                    ncpu = jobjson['CpusProvisioned']
                else:
                    ncpu = jobjson['RequestCpus']
                activedict['TotalCpus'] += ncpu
                try:
                    activedict['WeightedCpus'] += max(jobjson['RequestMemory']/memory_slot, ncpu)
                except:
                    #try to fix it by forcing an evaluation of the RequestMemory classad of the job
                    try:
                        activedict['WeightedCpus'] += max(ijob.eval("RequestMemory")/memory_slot, ncpu)
                    except:
                        print(f"{start_time}: Still a problem with WeightedCpus")
                        activedict['WeightedCpus'] += ncpu
                activedict['TotalRemoteUserCpu'] += jobjson['RemoteUserCpu']
                activedict['TotalWallTime'] += (tprev - jobjson['EnteredCurrentStatus'])*ncpu
            elif jobjson['JobStatus'] == 5:
                activedict['Held'] += 1
    nsamples += 1

# Now we have the json, let's push the record to the index, if we actually found anything

nsamples = max(nsamples, 1)

if True:
#if totals['AllIdle'] > 0 or totals['AllRunning'] > 0 or totals['AllHeld'] > 0:
    for totkey, totval in totals.items():
        # we are out of the while loop, so we have to divide the totals by the number of samples
        if totkey != "timestamp":
            for okey, oval in totval.items():
                for bkey, bval in oval.items():
                    for labkey, labval in bval.items():
                        tempdict = {"Owner": okey, "site": totkey, "bps_run": bkey,
                                    "bps_job_label": labkey, "timestamp": totals['timestamp']}
                        for jkey, jval in labval.items():
                            if jkey != "Efficiency":
                                labval[jkey] = 1.0*jval/nsamples
                                tempdict[jkey] = labval[jkey]
                        if labval['TotalWallTime'] > 0:
                            labval['Efficiency'] = 1.0*labval['TotalRemoteUserCpu']/labval['TotalWallTime']
                            tempdict['Efficiency'] = labval['Efficiency']
                        else:
                            labval['Efficiency'] = 0
                            tempdict['Efficiency'] = 0
                        dictlist.append(tempdict)
    if debug:
        print(dictlist)

 # Finally, push the record
try:
    for idict in dictlist:
        result = client.index(index=os_index_name, body=idict)
except Exception as e:
    logtime = datetime.datetime.fromtimestamp(start_time)
    print(f"{logtime}: Error uploading json to {os_index_name}.")
    print("Failure: " + str(e))
