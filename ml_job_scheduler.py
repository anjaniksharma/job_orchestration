##################################################################################################
# Author: Anjani Kumar Sharma                                                                     #
# Date: 8/1/2019                                                                                  #
#################  Machine Learning Training Data Scheduler  ######################################
# This script schedules various other scripts that will pull from oracle or Hbase, run hql to     #
# transform ods table into de-normalized structures & finally training views. The script makes job# 
# entries from ETL_ML_DATASET_LIST to ETL_ML_DATASET_TRACK_LOG & loops through the various steps. #
# The dependencies & parallelism is defined in GROUP_RUN_ORDER & GROUP_IN_PARALLEL resp.The status#
# of each script is tracked.                                                                      #
###################################################################################################

"""
Change Log:


"""



import os, sys, re,cx_Oracle, errno,logging
import time, datetime
from socket import *
import psutil,shutil
import math
import subprocess
from subprocess import Popen, PIPE
from NPDLogger import NPDLogger
import ConfigParser
import traceback
import platform
import threading
from ml_navclass_service_scheduler import run_sql,get_db_credential,get_db_connection,send_mail


work_serial = os.getenv('WORK_SERIAL')
time_str = time.strftime('%m%d%Y')
email_recepients = os.getenv('Email_Recipient')

server_name = platform.node().split(".")[0]
log_severity_level = 'debug'
log_file_name = "ml_navclass_training_data_scheduler_"+server_name
log_file_name_path = work_serial + "/ml_logs/ml_navclass_training_data_scheduler_"+server_name + "_"+time_str +".log"
log = NPDLogger(log_file_name=log_file_name,log_severity_level = log_severity_level).log()


envir=os.getenv('STAGE')
if not envir:
    log.error(".profile parameter STAGE not set")
    raise NameError( "get_db_credential ->  .profile parameter STAGE not set")
home = os.getenv('HOME')
if not home:
        log.error(".profile parameter HOME not set")
        raise NameError( "get_db_credential ->  .profile parameter STAGE not set")


env = os.environ.copy()
email_base_subject = " "+envir.upper() +": ML Training View : " + server_name +" : "

line_start = "\n*************************************** GROUP START *******************************************************"
line_msg = "*************************************** %s *******************************************************"
line_end =   "\n**************************************** GROUP END ********************************************************"
def get_session_id():
    sql_str = "select ETL_MGR.SEQ_ETL_ML_DATASET_TRCK_LOG.nextval from dual"
    status, res = run_sql(sql_str =sql_str, param_dict={})
    if status != 0:
        log.error(res)
        raise NameError(res)
    else:
        return res[0][0]

def get_date_id():
    sql_str = "select year||lpad(month,2,0)||week from ods.ods_calendar_weeks where calendar_id =1 and sysdate between week_start_date and week_end_date"
    status, res = run_sql(sql_str =sql_str, param_dict={})
    if status != 0:
        log.error(res)
        raise NameError(res)
    else:
        return res[0][0]
    
def create_job_items():
    date_id = get_date_id()
    def create_dataset_job_entry(session_id, date_id):
        sql_str = "insert into ETL_MGR.ETL_ML_DATASET_TRACK_LOG (SESSION_ID, DATE_ID, DATASET_ID, STATUS, TRACK_STAGE_TYPE, START_TIME, END_TIME, ADDED_DATE, ADDED_USER, GROUP_RUN_ORDER, GROUP_IN_PARALLEL) select :session_id,  :date_id,dataset_id,:status,null,sysdate, null,sysdate,'etl_user',GROUP_RUN_ORDER, GROUP_IN_PARALLEL from ETL_MGR.ETL_ML_DATASET_LIST where active_flag =1 and dataset_id not in (select dataset_id from ETL_MGR.ETL_ML_DATASET_TRACK_LOG where session_id = :session_id and date_id = :date_id)"
        param_dict = {"session_id": session_id, "status":0, "date_id":date_id}
        status, res = run_sql(sql_str =sql_str, param_dict=param_dict,stmt_type='insert' )
        if status != 0:
            raise NameError("Error in create_dataset_job_entry: %s" %res)
    
    sql_str = "select max(session_id) from ETL_MGR.ETL_ML_DATASET_TRACK_LOG where date_id = :date_id"
    param_dict = {"date_id":date_id}
    status, res = run_sql(sql_str =sql_str, param_dict=param_dict )
    
    if status ==0:
        if res[0][0]:
            session_id = res[0][0]
            create_dataset_job_entry(session_id = session_id, date_id = date_id)
        else:
            session_id = get_session_id()
            create_dataset_job_entry(session_id = session_id, date_id = date_id)
    else:
        log.error("Error in create_job_items: %s" %res)
        raise NameError("Error in create_job_items: %s" %res)
        
   
    
def update_dataset_job_fields(session_id,date_id, dataset_id,update_dict):
    sql_str1 = "update ETL_MGR.ETL_ML_DATASET_TRACK_LOG set "
    sql_str2 = ""
    for key in update_dict.keys():
        if key.find("time") > 0:
            sql_str2 = sql_str2 + ' , '+key+' = '+ update_dict[key]
            del update_dict[key]
        else:            
            sql_str2 = sql_str2 + ' , '+key+' = :'+ str(key)
    sql_str2 = re.sub('^ ,',' ',sql_str2)
    sql_str3 = " where session_id = :session_id and dataset_id = :dataset_id and date_id = :date_id"
    sql_str = sql_str1 + sql_str2 + sql_str3
    param_dict = {"session_id": session_id, "dataset_id": dataset_id, "date_id":date_id}
    param_dict.update(update_dict)
    
    status, res = run_sql(sql_str =sql_str, param_dict=param_dict,stmt_type='update' )
    if status != 0:
        log.error("Error in update_dataset_job_fields: %s" %res)
        raise NameError("Error in update_dataset_job_fields: %s" %res)


    
def execute_jobs(session_id,date_id, dataset_id):
    def get_cmd_str(dataset_id):
        sql_str = "select trim(script_name) from ETL_MGR.ETL_ML_DATASET_LIST where dataset_id = :dataset_id"
        param_dict = {"dataset_id":dataset_id}
        status, res = run_sql(sql_str =sql_str, param_dict=param_dict)
        if status != 0:
            raise NameError("Error in get_cmd_str: %s" %res)
        else:
            return res[0][0]
    
    scrpt = get_cmd_str(dataset_id)
    cmd = ['ksh',scrpt]
    
    try:
        log.info("Starting script: %s" %scrpt)
        #print ("Starting script: %s" %scrpt)
        p = Popen(cmd,shell=False, env = env,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        update_dataset_job_fields(session_id=session_id,date_id =date_id, dataset_id = dataset_id,update_dict ={"start_time":"sysdate","status":1, "process_info":"Started","process_id": p.pid})
        output = ""
        for line in iter(p.stdout.readline, ""):
            output += line
        
        p.wait()
        exitCode = p.returncode
        log.warn(output)
        if exitCode == 0:
            log.info("Success script: %s" %scrpt)
            #print ("Success script: %s" %scrpt)
            update_dataset_job_fields(session_id=session_id,date_id =date_id, dataset_id = dataset_id,update_dict ={"end_time":"sysdate","status":2, "process_info":"Success"})
        else:
            log.error(output)
            update_dataset_job_fields(session_id=session_id,date_id =date_id, dataset_id = dataset_id,update_dict ={"end_time":"sysdate","status":-1, "process_info":"Failed"})
            log.info("Failed: script: %s" %scrpt)
            #print ("Failed: script: %s" %scrpt)
    except OSError,e:
        subject = "Error:"+email_base_subject+" Cannot fork a job"
        body = "Cannot start the job -> %s" %(cmd)
        body = body + "\n" + ("OS ERROR -> %s" %e)
        send_mail(email_recepients=email_recepients,subject=subject,body= body)
        log.error(body)
        update_dataset_job_fields(session_id=session_id,date_id =date_id, dataset_id = dataset_id,update_dict ={"status":-1, "process_info":"OS Error - Failed"})
    except: 
        subject = "Error:"+email_base_subject+" Error in executing script"
        body = "error in executing script -> %s" %(cmd)
        body = body + "\n" + ("OS ERROR -> %s" %sys.exc_info()[0])
        log.error(body)
        send_mail(email_recepients=email_recepients,subject=subject,body= body)
        update_dataset_job_fields(session_id=session_id,date_id =date_id, dataset_id = dataset_id,update_dict ={"status":-1, "process_info":"Other OS Error - Failed"})

        
def parallel_thread_jobs(list_of_jobdef):
    threads = []
    for session_id,date_id, dataset_id, group_run_order,group_in_parallel in list_of_jobdef:
        t = threading.Thread(target =execute_jobs, args=(session_id,date_id, dataset_id))
        t.daemon = True
        t.name = 'sess_date_dataset_id_'+str(session_id)+'_'+str(date_id)+'_'+str(dataset_id)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    
    

def check_previous_job_status(group_run_order, session_id):
    sql_str = """
    SELECT count(*) dataset_list
  FROM etl_mgr.ETL_ML_DATASET_TRACK_LOG where status = -1 and group_run_order < :group_run_order and session_id = :session_id
    """
    param_dict = {"group_run_order": group_run_order, "session_id":session_id}
    status, res = run_sql(sql_str =sql_str, param_dict=param_dict)
    if status != 0:
        log.error("Error in check_previous_job_status: %s" %res)
        raise NameError("Error in check_previous_job_status: %s" %res)
    if res[0][0] == 0:
        return
    else:
        sql_str = """update etl_mgr.ETL_ML_DATASET_TRACK_LOG  set status= -1, process_info ='Failed due to parent job failure' 
        where group_run_order >= :group_run_order and session_id = :session_id"""
        param_dict = {"group_run_order": group_run_order, "session_id":session_id}
        status, res = run_sql(sql_str =sql_str, param_dict=param_dict,stmt_type='update')
        if status != 0:
            log.error("Error in check_previous_job_status - update status: %s" %res)
            raise NameError("Error in check_previous_job_status- update status: %s" %res)
        
        raise Exception("Previous job failed: session_id: %s - group_run_order: %s" %(session_id, group_run_order))
        
def start_job_execution_threads():
    date_id=get_date_id()

    sql_str = """SELECT LISTAGG(a.session_id||'|'||a.date_id||'|'||a.dataset_id||'|'||a.group_run_order||'|'||a.group_in_parallel, ',')
WITHIN GROUP (ORDER BY a.group_in_parallel, a.group_run_order)  dataset_list
  FROM ETL_MGR.ETL_ML_DATASET_TRACK_LOG a, etl_mgr.etl_ml_dataset_list b where a.dataset_id = b.dataset_id and b.active_flag=1 and a.status =0 and a.date_id = :date_id and a.session_id = (select max(session_id) from 
  ETL_MGR.ETL_ML_DATASET_TRACK_LOG where date_id = :date_id )
  GROUP BY a.group_in_parallel,a.group_run_order
    ORDER BY a.group_run_order """
    param_dict = {"date_id" : date_id}
    status, res = run_sql(sql_str =sql_str, param_dict=param_dict)
    
    if status == -1 or status == -2:
        raise NameError("Error in start_job_execution_threads: %s" %res)
    elif status == -3:
        log.info("No Job scheduled")
        return
    else:
        job_list = res
        
    for job_item in job_list:
        
        list_of_jobdef = [jb.split("|") for jb in job_item[0].split(",")]
        group_run_order = list_of_jobdef[0][3]
        session_id = list_of_jobdef[0][0]
        group_in_parallel = list_of_jobdef[0][4]
        
        check_previous_job_status(group_run_order, session_id)
        log.info(line_start)
        #print(line_start)
        log.info("\n***************************** GROUP RUN ORDER: %s === GROUP IN PARALLEL: %s ***********************************************" %(group_run_order,group_in_parallel))
        parallel_thread_jobs(list_of_jobdef)
        log.info(line_end)

        
def check_self():
    ml_navclass_training_data_scheduler = home + '/'+ envir + '/npd_batch/machine_learning/ml_navclass_training_data_scheduler.py'
    cmd = ['python' ,ml_navclass_training_data_scheduler]
    cmd1 = ['python' ,'ml_navclass_training_data_scheduler.py']
    proc_count = 0
    for process in psutil.process_iter():
        if process.cmdline() == cmd or process.cmdline() == cmd1:
            proc_count += 1
    if proc_count > 1:
        log.info("Instance already running. Exiting")
       	sys.exit(0)
if __name__=="__main__":
	check_self()
	log.info("\n*******************  START ITETATION ************************")
	create_job_items()
	start_job_execution_threads()
	log.info("\n*******************  END ITETATION ************************")
