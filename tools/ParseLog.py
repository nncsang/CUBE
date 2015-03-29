import commands
import json
import os
import sys
import MongoConn

PATHs_file = open('environment.json','r')
PATHs = json.load(PATHs_file)
HADOOP_HOME = PATHs['HADOOP_HOME']
LOG_DIR = PATHs['LOG_DIR']
LOG_JSON_DIR = PATHs['LOG_JSON_DIR']

Rumen_libs = HADOOP_HOME+'/hadoop-tools-1.2.1.jar:'+HADOOP_HOME+'/hadoop-core-1.2.1.jar:'+HADOOP_HOME+'/lib/*'

RefineDB = 'terasort_refine'


dbconn = MongoConn.DbConn()  
conn = None

def extract_configs():
	config_file = open('config.json','r')
	config_json = json.load(config_file)
	configs = config_json['sample_standard_list']
	return configs



class Log:
	def __init__(self,log_path,inputdata_size):
		self.log_path = log_path
		self.inputdata_size = inputdata_size
		self.job_id = self.GetJobID(log_path)

	def GetJobID(self,log_path):
		cmd = "ls "+log_path+"/history | grep conf.xml | sed 's/_conf.xml//g'"
		return str(commands.getoutput(cmd))

	def GetExecTime(self,raw_log_json):
		finishTime = -1	
		for other_task in raw_log_json['otherTasks']:
			if other_task["taskType"] == "SETUP":
				#	print "start"	
				startTime=other_task["startTime"]
				#	print startTime
			if other_task["taskType"] == "CLEANUP":
				#        print "finish"
				finishTime=other_task["finishTime"]
				#	print finishTime

		if finishTime==-1:
			exec_time = -1
		else:	
			exec_time = finishTime-startTime

		return exec_time

	def GetMapExecTime(self,raw_log_json):
		map_execTime_list = []
		for map_task in raw_log_json['mapTasks']:
			if map_task['taskStatus'] == 'SUCCESS':
				map_execTime = map_task['finishTime']-map_task['startTime']
				map_execTime_list.append(map_execTime)
		return map_execTime_list

	def GetRedExecTime(self,raw_log_json):
		reduce_execTime_list = []
		for reduce_task in raw_log_json['reduceTasks']:
			if reduce_task['taskStatus'] == 'SUCCESS':
				reduce_execTime = reduce_task['finishTime']-reduce_task['startTime']
				reduce_execTime_list.append(reduce_execTime)
		return reduce_execTime_list

	def Raw_to_RawJson(self):
		cmd = 'java -cp '+Rumen_libs+' org.apache.hadoop.tools.rumen.TraceBuilder file://'+LOG_JSON_DIR+'/out_'+self.job_id+'.json file://'+LOG_JSON_DIR+'/topo_'+self.job_id+' file://'+self.log_path+'/history'
		os.system(cmd)




	def Refine(self):
		raw_log_json_fstream = open(LOG_JSON_DIR+'/out_'+self.job_id+'.json')
		raw_log_json = json.load(raw_log_json_fstream)

		configs = extract_configs()
		JobProperty = raw_log_json['jobProperties']
		refine_config_set = {}
		for conf in configs:
			conf_abbr = conf['abbr']  #the abbreviation of config parameter
			#remove 2 space char in conf name in config.json
			conf_name = conf['name'].replace(' ','') 
			refine_config_set.update({str(conf_abbr):JobProperty[conf_name]})



		exec_time = self.GetExecTime(raw_log_json)				

		map_execTime_list = self.GetMapExecTime(raw_log_json)
		reduce_execTime_list = self.GetRedExecTime(raw_log_json)



		
		refine_log_data = {'job_name':raw_log_json['jobName'],'job_id':self.job_id,'config_set':refine_config_set,'data_size':self.inputdata_size,'exec_time':exec_time,'map_exec_time':map_execTime_list,'reduce_exec_time':reduce_execTime_list}
		raw_log_json_fstream.close()	
		return refine_log_data





	def RefineJson_SaveIn_DB(self):
		
		dbconn.connect()
		global conn
		conn = dbconn.getConn()

		BigDB = conn.big
		
		collection = BigDB[RefineDB]
		refine_log_data = self.Refine()
		insert_id =collection.insert(refine_log_data)

	


