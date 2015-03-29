#!/usr/bin/env python

import os
import os.path
import re
import sys
import json
from optparse import OptionParser
from datetime import datetime

class eaggTask:
    def __init__(self,optype,level,id,jobid,**args):
        self.optype = optype
        self.level = int(level)
        self.id = int(id)
        self.jobid = jobid
        self.name = "%s-%d-%d" % ( self.optype, self.level, self.id )
        self.begin = None
        self.end = None
        self.success = False
        self.taskid = None
        self._set_taskid()
        self.key = ( self.optype, self.level, self.id )

    def _set_taskid(self):
        if self.optype not in ('WORK','MERGE'):
            return
        taskprefix = 'task' + self.jobid[3:]
        if self.optype == 'WORK':
            self.taskid = taskprefix + '_m_000000'
        else:
            self.taskid = taskprefix + '_r_000000'
        #print 'setting taskid %s' % self.taskid

    def toJson(self,x):
        return { 'op' : self.optype, 'lv' : self.level, 'id' : self.id, 'x' : (self.begin - x).seconds, 'runtime': self.getRuntime() }

    def getRuntime(self):
        delta = self.end - self.begin
        return delta.seconds + delta.microseconds / 1000000.0

    def __str__(self):
        return "%s %s %s %.3f" % ( self.name, self.begin, self.end, self.getRuntime() )

def parse_time(dateStr,timeStr):
    (year, month, day) = dateStr.split('-')
    (hour, minute, secs, msecs) = timeStr.replace(',',':').split(':')
    return datetime(int(year),int(month,10),int(day,10),int(hour,10),int(minute,10),int(secs,10),int(msecs,10)*1000);

def to_seconds(f,t):
    delta = t - f
    return delta.seconds + delta.microseconds / 1000000.0

def statistics(header,tasks):
    jobBegin = min( map(lambda l: l.begin, tasks) )
    jobEnd = max( map(lambda l: l.end, tasks) )

    runtimes = map(lambda l: l.getRuntime(), tasks)
    runtimes.sort()
    n = len(tasks)
    avg = sum( runtimes, 0.0 ) / n

    print >> sys.stderr, "%s,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f" % (header, to_seconds(jobBegin,jobEnd),runtimes[0], runtimes[int(0.05*n)], runtimes[int(0.5*n)], runtimes[int(0.95*n)], runtimes[-1], avg )

    return ( jobBegin, jobEnd )

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-l','--log',dest='eagg',help='EAGG log file')
    parser.add_option('-j','--jt',dest='jt',help='Job tracker log file')
    parser.add_option('-o','--out',dest='output',help='output file',default=None)
    parser.add_option('-p','--prefix',dest='prefix',help='prefix of html output',default=None)

    (options,args) = parser.parse_args()

    if options.jt is None or options.eagg is None:
        parser.error('both -l and -j must present')

    if not os.path.exists( options.eagg ) or not os.path.exists( options.jt ):
        parser.error('log files are missing')

    output = sys.stdout
    if options.output is not None:
        output = open(options.output,'w')

    if options.prefix is None:
        options.prefix = options.eagg
    
    # first load mapping
    TASKINDEX = {}
    JOBID2EAGGTASK = {}
    TASKID2EAGGTASK = {}

    MAPPING_PATTERN = re.compile('\sEAGG-MR\s+(?P<optype>\w+)-(?P<level>\d+)-(?P<id>\d+)\s+(?P<jobid>job_\d+_\d+)$')
    TIMING_PATTERN = re.compile('\sEAGG-MR\s+(?P<optype>\w+)-(?P<level>\d+)-(?P<id>\d+)\s+(?P<jobid>job_\d+_\d+)\s+(?P<beginTime>\d+)\s+(?P<endTime>\d+)$')

    JOBID_PATTERN = re.compile('(?P<jobid>job_\d+_\d+)')
    TASKID_PATTERN = re.compile('(?P<taskid>task_\d+_\d+_(m|r)_\d+)')

    eaggLog = open(options.eagg,'r')
    for l in eaggLog:
        m1 = MAPPING_PATTERN.search(l)
        m2 = TIMING_PATTERN.search(l)

        if m1 is not None:
            task = eaggTask(**m1.groupdict())
            TASKINDEX[task.key] = task
            JOBID2EAGGTASK[task.jobid] = task
            if task.taskid is not None:
                TASKID2EAGGTASK[task.taskid] = task
        elif m2 is not None:
            task = eaggTask(**m2.groupdict())
            TASKINDEX[task.key] = task
            JOBID2EAGGTASK[task.jobid] = task
            if task.taskid is not None:
                TASKID2EAGGTASK[task.taskid] = task

            #key = ( m2.group('optype'), int(m2.group('level')), int(m2.group('id')) )
            #task = TASKINDEX[key]
            task.begin = datetime.fromtimestamp( long(m2.group('beginTime'))/1000.0 + 10800)
            task.end = datetime.fromtimestamp( long(m2.group('endTime'))/1000.0 + 10800)

        #print "Mapping: %s %s %s -> %s" % m.group('optype','level','id','jobid')
    eaggLog.close()

    numPartitions = 1 << max(map(lambda t: t.level,TASKINDEX.values()))

    print >> sys.stderr, "Number of partitions = %d" % numPartitions

    # load job tracker log
    jtLog = open(options.jt,'r')
    for l in jtLog:
        if not l.startswith('2'):
            continue

        (datestr, timestr, loglevel, source, msg) = l.rstrip().split(' ',4)
        if not source.endswith('JobInProgress:'):
            continue

        if msg.startswith('Initializing'):
            m = JOBID_PATTERN.search(msg)
            if m is None or m.group(0) not in JOBID2EAGGTASK:
                continue
            task = JOBID2EAGGTASK[m.group(0)]
            if task.begin is None:
                task.begin = parse_time(datestr,timestr)

        if msg.startswith('Choosing'):
            m = TASKID_PATTERN.search(msg)
            if m is None or m.group(0) not in TASKID2EAGGTASK:
                continue
            task = TASKID2EAGGTASK[m.group(0)]
            if task.begin is None:
                task.begin = parse_time(datestr,timestr)

        if msg.endswith('completed successfully.'):
            if msg.startswith('Job'):
                m = JOBID_PATTERN.search(msg)
                if m is None or m.group(0) not in JOBID2EAGGTASK:
                    continue

                task = JOBID2EAGGTASK[m.group(0)]
                if task.end is None:
                    task.end = parse_time(datestr,timestr)

                JOBID2EAGGTASK[m.group(0)].success = True
            elif msg.startswith('Task'):
                m = TASKID_PATTERN.search(msg)
                if m is None or m.group(0) not in TASKID2EAGGTASK:
                    continue
                task = TASKID2EAGGTASK[m.group(0)]
                if task.end is None:
                    task.end = parse_time(datestr,timestr)
 
        if msg.startswith('Killing'):
            m = JOBID_PATTERN.search(msg)
            if m is None or m.group(0) not in JOBID2EAGGTASK:
                continue
            if JOBID2EAGGTASK[m.group(0)].end is None:
                JOBID2EAGGTASK[m.group(0)].end = parse_time(datestr,timestr)
            JOBID2EAGGTASK[m.group(0)].success = False

    jtLog.close()

    sampleTask = []
    partitionTask = []
    workTask = []
    mergeTask = []

    print >> output,"numPart,op,lv,id,begin,end,runtime,success"

    for job in JOBID2EAGGTASK.values():
        if job.end is None:
            continue
        print >> output,"%d,%s,%d,%d,%s,%s,%.3f,%d" % ( numPartitions, job.optype, job.level, job.id, str(job.begin)[:-3], str(job.end)[:-3], job.getRuntime(), job.success )
        if job.optype == 'MERGE':
            mergeTask.append(job)
        elif job.optype == 'WORK':
            workTask.append(job)
        elif job.optype == 'SAMPLE':
            sampleTask.append(job)
        else:
            partitionTask.append(job)

    statistics("SAMPLE",sampleTask)
    statistics("PARTITION",partitionTask)
    statistics("WORK",workTask)
    statistics("MERGE",mergeTask)
    jb, je = statistics("TOTAL",JOBID2EAGGTASK.values())

    # output json object
    jsonObjs = map(lambda t : t.toJson(jb), JOBID2EAGGTASK.values())
    width = (je - jb).seconds
    height = len(jsonObjs)
    lineWidth = 1
    #while height < 768:
    #    lineWidth = lineWidth + 1
    #    height = len(jsonObjs) * lineWidth
    
    jsonOut = open( options.prefix + '.js', 'w')
    jsonOut.write('var chartData = ');
    json.dump({ 'width' : width, 'height' : height, 'lineWidth' : lineWidth, 'tasks' : jsonObjs },jsonOut,sort_keys=True)
    jsonOut.write(';');
    jsonOut.close()

    # output html template
    htmlOut = open( options.prefix + '.html', 'w')
    htmlOut.write("""<html>
<head>
<script type="text/javascript" src="%s.js"></script>
<script type="text/javascript" src="/homes/yongchul/drawGantt.js"></script>
</head>
<body onload="sortByBeginTimeAndRedraw()">
<canvas id="fig"></canvas>
</body>
</html>""" % options.prefix);
    htmlOut.close();

    if options.output is not None:
        output.close()