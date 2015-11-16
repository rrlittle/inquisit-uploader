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
##
##
##  ERROR CODES:
##  2 - argument error probably
##  3 - changing working dir did not work
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
from os import chdir, getcwd,listdir



#
#   ARGUMENTS
#

argparser = argparse.ArgumentParser(description = 'Uploads inquisit datafiles to tables in wtp_data')
argparser.add_argument('-t','--table', required=True)
argparser.add_argument('-f','--file_pattern',required=True)
argparser.add_argument('-p','--path_to_working_dir',required=True)
argparser.add_argument('-d','--dsn',required=False, default='wtp_data')
argparser.add_argument('--debug',required=False, default=None)


args = vars(argparser.parse_args()) # get the arguments from argparse

tablename = args['table']
rawfile_name = args['file_pattern']
dsn = args['dsn']
working_dir = args['path_to_working_dir']
debug = args['debug']


#
#   Go to the working dir
#


try:
    print('starting at:' + getcwd())
    print( 'attempting to move to ' + working_dir)
    chdir(working_dir)
    print('now at: ' + getcwd())
    print('contains: ' + '\n'.join(listdir()))
except Exception as e:
    print('moving to workingdir did not work error -> ', e)
    sys.exit(3)

#
#   LOGS
#       Redirect stdout to the logfile. as this is usually going to be run by a scheduled task. 

logname = time.asctime().replace(' ','_').replace(':',".") # name it after the current time
sys.stdout = open('diag/upload_{}.txt'.format(logname), 'w')   # save the log


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
print('Found these datafiles. Trying to upload them.\n: {}'.format('\n: '.join(datfiles)))


#
#   Processing datafiles
#
saved_files = []
error_list = [] # container for files that were not uploaded successfully
for f_i,f in enumerate(datfiles):
    try:
        file_inserted_without_errors = True # if anything goes wrong flip this. but default to true 
        print('===================\tprocessing data file {}/{}\t==================='.format(f_i, len(datfiles)))
        print('========\t{}\t========\n'.format(f))
        fo = open(f,'r')
        r = csv.reader(fo, delimiter='\t')
        l = [line for line in r]
        fo.close() # close the file object. it's been read in already 
      

        #
        #   GO THROUGH ROWS
        #

            #   to be filled with tuples (row, error string)
        for i,line in enumerate(l[1:]): # iterate through each row
            sys.stdout.flush() # refresh the log.

            # encapsulate strings with quotes
            for j,v in enumerate(line): 
                if coltypes[j] is str: 
                    line[j] = "'{}'".format(v.replace("'",'')) # in inquisit tasks ctrl keypresses show up as varchars "ctrl + 'Q'" 
            
            # construct the insert statement                    
            statement = 'INSERT INTO {} SET '.format(tablename)
            for j in range(len(line)):
                statement += '{} = {}, '.format(colnames[j], line[j])
            statement = statement[:-2] +  ';' # remove ', ' from the end of the string


            # insert the row
            try:
                cur.execute(statement)
                cur.commit()
                # if it did not succeed breaks out of try. and go to the next one. 
                # else continue
                # to save space in logfile don't print this
                if debug: print('inserted row {}/{} of file {} __successfully__. WITH STATEMENT: {}'.format(i,len(l)-2,f, statement))

            except Exception as e:
                print('\n__error__ inserting row {}/{} into table {}. \terror: {}'.format(i,len(l)-2,f,e))
                print(statement + '\n')
                file_inserted_without_errors = False # set flag to say it was completed with errors
                error_list.append((f, 'ERROR WHILE INSERTING ROW' , e))
        
        #
        # IF NO ERRORS MOVE TO SAVED DIRECTORY
        #
        if file_inserted_without_errors:
            # then move datafile into the saved directory.
            try:
                shutil.move(f, 'saved/{}'.format(f))
                print('NO ERRORS.')
                saved_files.append(f)
            except Exception as e:
                print('__Error__ moving the datafile to the saved directory. Error:{}'.format(e))
         
    except Exception as e:
        print('\n------------------------')
        print('\t__UNKNOWN ERROR__ WHILE PROCESSING FILE!!! not while moving or executing statement. \nError:{}'.format(e))
        print('=========================\n\n\n')
        error_list.append((f,'UNKNOWN ERROR',e))


if len(error_list) > 0:
    print('\n\n\n\n##FILES that were __not__ successfully saved:\n=================')
    prevfile = ''
    for f in error_list:
        if f[0] == prevfile:
            f[0] = '^'*len(prevfile)
        else:
            prevfile = f[0]
        print('type:',f[1], '-> file:',f[0],'error:',f[2])

if len(saved_files) > 0:
    print('\n\n\n\n##FILES that were successfully saved:\n=================')
    for f in saved_files:
        print(f)    
    
f.close()
cur.close()
con.close()