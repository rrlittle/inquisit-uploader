############################
##  Authors: Russell Little, Praneeth Naramsetti
##  Oct 12 2015
##  this script is designed to back up raw inquisit datafiles into wtp_database
##
##  SETUP & REQUIREMENTS
##  place this script in the directory with the inquisit datafiles.
##  datafiles should be named something like EXPERIMENT_rawdata_identifier.iqx
##  
##  also create a diag directory and saved directory in the same folder. 
##  diag will hold the run logs. to diagnose any erros that occur.
##  saved will hold the rawdata files after they have been saved to the database.
##
##  This program should be run nightly or weekly or something like that it takes a little while to run.
##
##  It requires python 3 & pypyodbc to be installed. all other packages are not installed with python3 
##  
##
##  this saves the data into a single table update the table it should save into in the tablename variable below
##  this uses a DSN to connect to the database. update the name of the dsn below. FOR the WTP you shouldn't change it.
##  this uploads all files in this directory matching a filename pattern. update that pattern in rawfilename below
##########################


# YOU SHOULDN"T HAVE TO CHANGE BELOW THIS!!!
#==========================================
import argparse
import glob # used to grab file names in directory
import csv  # used to parse csv
import pypyodbc as db # used to connect to database
import shutil # used  to move files to saved directory to mark them as saved
import sys #used to log errors 
import time # used to generate log names

#
#   LOGS
#       Redirect stdout to the logfile. as this is usually going to be run by a scheduled task. 

logname = time.asctime().replace(' ','_').replace(':',".") # name it after the current time
sys.stdout = open('diag/{}.txt'.format(logname, 'w'))   # save the log


#
#   ARGUMENTS
#

argparsaser = argparse.ArgumentParser(description = 'Uploads inquisit datafiles to tables in wtp_data')
p.add_argument('-t','--table', required=True)
p.add_argument('-f','--file_pattern',required=True)
p.add_argument('-d','--dsn',required=False, default='wtp_data')

args = vars(p.parse_args())

tablename = args['table']
rawfile_name = args['file_pattern']
dsn = args['dsn']

print('PROVIDED ARGS: {}'.format(args))



#
#   DATABASE CONNECTION
#

con = db.connect(DSN=dsn) # connect to wtp_data
cur = con.cursor()


cur.execute('SELECT * FROM {}'.format(tablename))
desc = cur.description

colnames = ['`{}`'.format(d[0]) for d in desc]

coltypes = [d[1] for d in desc] # get the column types to decide if we need to encapsulate them or not.


#
#   GET DATAFILES
#

datfiles = glob.glob(rawfile_name) # grab all files that need to be uploaded.
print('Found these datafiles. Trying to upload them.\n{}'.format(datfiles))


#
#   Processing datafiles
#

failed_files = [] # container for files that were not uploaded successfully
for f in datfiles:
    file_inserted_without_errors = True # if anything goes wrong flip this. but default to true 
    try:
        print('===================\n\tprocessing data file {}\n===================\n'.format(f))
        fo = open(f,'r')
        r = csv.reader(fo, delimiter='\t')
        l = [line for line in r]
        fo.close() # close the file object. it's been read in already 
        
        for i,line in enumerate(l[1:]): # iterate through each row
            sys.stdout.flush()
            # encapsulate strings with quotes
            for j,v in enumerate(line): 
                if coltypes[j] is str: 
                    line[j] = "'{}'".format(v.replace("'",'')) # in inquisit tasks ctrl keypresses show up as varchars "ctrl + 'Q'" 
            
            # construct the insert statement                    
            statement = 'INSERT INTO {} SET '.format(tablename)
            for j in range(len(line)):
                statement += '{} = {}, '.format(colnames[j], line[j])
            statement = statement[:-2] +  ';'


            # insert the row
            try:
                cur.execute(statement)
                # if it did not succeed breaks out of try. and go to the next one. 
                # else continue
                # to save space in logfile don't print this
                #print('inserted row {}/{} of file {} __successfully__'.format(i,len(l)-2,f))

            except Exception as e:
                print('\n__error__ inserting row {}/{} into table {}. from file: {}\n\terror: {}'.format(i,len(l)-2,tablename,f,e))
                print(statement + '\n\n')
                file_inserted_without_errors = False # set flag to say it was completed with errors
        # after inserting file.
        if file_inserted_without_errors:
            # then move datafile into the saved directory.
            try:
                shutil.move(f, 'saved/{}'.format(f))
                print('NO ERRORS.')
            except Exception as e:
                print('__Error__ moving the datafile to the saved directory. Error:{}'.format(e))
        else: # if file has some errors in it. save the filename 
            failed_files.append((f,e)) # use this to print out a table at the end
    except Exception as e:
        print('\n------------------------')
        print('\t__ERROR__ WHILE PROCESSING FILE!!! not while moving or executing statement. Error:{}'.format(e))
        print('=========================\n\n\n')
        failed_files.append((f,e))

print('\n\n\n\n##FILES that were not successfully saved:\n=================')
for f in failed_files:
    print('file:',f[0],'error:',f[1])
    


