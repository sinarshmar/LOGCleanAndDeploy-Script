import pandas as pd
import pysftp
import os
import threading
import time
from pyspark.sql import SparkSession
from xon import xon
import datetime
import logging as log

# dataframe display settings
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 150)

# Importing csv containing successfully parsed files names.
df_s = pd.read_csv("success_records.csv")

# Importing csv with error file names.
df_err = pd.read_csv("error_records.csv")

# importing the bank repository.
df_r = pd.read_csv("Bank_repository.csv")

def main():

    # Getting details of the Bank whose logs will be uploaded.
    global Bank, Platform, Timezone, Log_l, sftp, host

    # Iterating through the rows of thee Bank details csv(Bank_repository.csv)
    for row in range(len(df_r)):
        Bank = df_r.loc[row, "Bank"]
        Platform = df_r.loc[row, "Platform"]
        Timezone = df_r.loc[row, "Timezone"]
        Log_l = df_r.loc[row, "Log_Address"]
        sftp = df_r.loc[row, "SFTP_Address"]
        host = df_r.loc[row, "Host"]

        print(" \n || Uploading logs from:",Bank,"|| \n")

        #calling fucntion to generate a listof of folders to be parsed.
        getUnparsedFolderList(sftp)

        # Defining threads to run executions.
        # Put a thread here for xop id generating function.
        t1 = threading.Thread(target=uploadAndGetID, args=[f_name_l])

        # Put a thread here to xon api to check the status of the xop provided id.
        t2 = threading.Thread(target=checkUploadStatus)

        t1.start()
        time.sleep(5)
        t2.start()

        t1.join()
        t2.join()

        while t1.isAlive() == True | t2.isAlive() == True:
            time.sleep(2)


        df_s.to_csv("success_records.csv", sep=",", index=False)
        df_err.to_csv("error_records.csv", sep=",", index=False)

        print("\n ||",Bank,"UPLOAD COMPLETE|| \n ________________________________________ \n ________________________________________")

    print("Success list \n", df_s, "\n")
    print("Error list \n", df_err)
##########################################

# Function to go to Log location and get the list of un-parsed folder and files.
def getUnparsedFolderList(sftp1):


    # Defining fucntion to check if the file being imported is more than 7 days old.
    def unix2human(unixtime):

        date_diff = (datetime.datetime.now().date() - datetime.datetime.utcfromtimestamp(int(unixtime)).date()).days

        try:
            return date_diff
        except Exception as e:
            log.warning("Failed to convert unixtime string '{}' :: {}".format(unixtime, e))
            return None

    user = "kumarutkarsh.singh"
    ##Not the real password
    pass1 = "----------"

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection(host=host, username=user, password=pass1, cnopts=cnopts) as sftp:

        sftp.cwd(Log_l)
        f_l1 = sftp.listdir()
        f_l2 = sftp.listdir_attr()

        # Since ASl uploads logs in multilevel folders. The list of folders will be in fomat: date_folder_name/DM_file
        if Bank == "ASL":
            f_l3=[]
            for file in f_l1:

                # index of current item in list.
                i = f_l1.index(file)

                # Checking if the file is older than 7 days.
                if unix2human(f_l2[i].st_mtime) > 7:
                   continue

                location= os.path.join(Log_l, file)
                sftp.cwd(location)
                n1= [ name for name in sftp.listdir() if name[0:2]=="DM"]

                s=file+"\\"+n1[0]
                if len(n1):
                    f_l3.append(s)

            f_l1=f_l3


    # Checking which folders are not already parsed and adding to list (f_name_l) to be sent to xop server.
    global f_name_l
    f_name_l = []

    for file in f_l1:
        fname = os.path.join(sftp1, file)
        if len(df_s[df_s.Path1 == fname]) == 0:
            f_name_l.append([fname])
        else:
            print("Excluding as already parsed:->", fname)
            continue

        # For ASL the folder age is already checked above.
        if Bank=="ASL":
            continue

        #index of current item in list.
        i =f_l1.index(file)

        # Checking if the file is older than 7 days.
        if unix2human(f_l2[i].st_mtime)>7:
            del f_name_l[-1]

    print(" \n \n List of folders to be parsed:->", f_name_l, "\n")


##########################################################


def uploadAndGetID(flist):
    # Storing function name we want to call.
    funcname = "xopServer_UploadAllFilesFromPath"

    global id_list
    id_list = []

    while (len(flist) > 0):

        if len(id_list) > 10:
            continue

        # Wait for the function to be available (with 10s timeout)
        xon.wait_until_available(timeout=10000, object=funcname)

        # Getting the offset from the timezone.
        #Offset= datetime.datetime.now(pytz.timezone(Timezone)).strftime('%z')

        #print(Offset)
        #Offset= Offset[:-2]+":"+Offset[-2:]
        #print(Offset)


        # Call the function
        id = xon.call_function(funcname, [flist[0],"ClientName", Bank,"PlatformName", Platform,"UtcOffset", Timezone], timeout=10000)

        # Changing from "response" datatype to string.
        id = id['Value']

        print("upload id =", id)

        # Append the received id in a list.
        id_list.append(id)

        # Removing uploaded file name from from the list.
        del flist[0]


######################################################

def checkUploadStatus():
    global df_s, df_err
    r=''

    print("Retrieving data from the xon Bus for xopServer...")
    while (len(id_list) > 0):
        # Wait for at least one publication to be available (with 10s timeout)
        xon.wait_until_available(timeout=10000, object="ANY.AUTO_UPLOAD.xopServer.{}".format(id_list[0]))

        # Retrieve the available records as Spark Dataframes and converting to Pandas dataframe.

        i1 = xon.snapshot(record="ANY.AUTO_UPLOAD.xopServer.{}".format(id_list[0])).toPandas()

        # Printing the received record.

        # Showing the progress in parsing every 10 seconds.
        if (i1.Status[0] == "In Progress"):
            if  r != i1.Id[0]:
                print("Record received :->","\t",i1.Id[0],"\t",i1.Status[0],"\t", i1.SftpPath[0], "\t")
                r=i1.Id[0]
                t =datetime.datetime.now()
            else:
                if (datetime.datetime.now() -t).total_seconds()>10:
                    print("In Progress")
                    t = datetime.datetime.now()

        else:
            print("Record received :->", "\t", i1.Id[0], "\t", i1.Status[0], "\t", i1.SftpPath[0], "\t")

        # Adding the received record to the csv files based on success or failure.
        if (i1.Status[0] == "In Progress"):
            continue
        elif (i1.Status[0] == "DONE"):
            row1 = pd.Series([i1.SftpPath[0], id_list[0]], index=df_s.columns)
            df_s = df_s.append(row1, ignore_index=True)
        elif (i1.DetailedStatus[0].find("have already been parsed") != -1):
            row1 = pd.Series([i1.SftpPath[0], id_list[0]], index=df_s.columns)
            df_s = df_s.append(row1, ignore_index=True)

        else:
            row2 = pd.Series([id_list[0], i1['SftpPath'][0], i1['DetailedStatus'][0]], index=df_err.columns)
            df_err = df_err.append(row2, ignore_index=True)

        del id_list[0]


if __name__ == "__main__":
    # Starting xon engine.
    spark = SparkSession.builder.appName("MyApp").getOrCreate()
    xon = xon(spark)
    xon.start()

    # Calling main function.
    main()

    xon.stop()
    spark.sparkContext.stop()

    # function to empty the error csv on 14th/30th of every month.
    if (datetime.datetime.now().strftime("%d") == '14' or datetime.datetime.now().strftime("%d") == '30'):
        df_err.drop(df_err.index, inplace=True)
        df_err.to_csv("error_records.csv", sep=",", index=False)
        print("\n \n Its 14th/30th today, so the error csv will be cleaned : \n ",df_err)




