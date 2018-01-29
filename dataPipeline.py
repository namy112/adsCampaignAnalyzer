from mainPipeline import Pipeline
import itertools
import csv
import pandas as pd
pipeline=Pipeline()

#This will reach each line in the log file and creates a list
@pipeline.task()
def read_file(log):
        
    lines=[]
    
    with log as f:
        for line in f:
            lines.append(line)
            
    return lines

"""T   his function reads in the line and separates out different columns based on space or some special
characters which will help us to uniquely identify each values"""
@pipeline.task(depends_on=read_file)
def split_fields(lines):   
    splitedLines=[]             
    for line in lines:
        split=line.replace(' ', '^^^').replace('&', '^^^').replace('=','^^^').split('^^^')
        splitedLines.append(split)
        
    return splitedLines
 
@pipeline.task(depends_on=split_fields)

#extracts specific fields that are needed for analysis purpose
def parse_log(splitedLines):
    
    for line in splitedLines:
        account_id=line[0]
        campaign_id=line[11]
        creative_id=line[13]
        date=line[1].replace('[','')
        impressions=line[7]
        if line[14]=='in_view_time':
            in_view_time =line[15] 
        else :
            in_view_time=''
        
        yield(
            account_id,campaign_id,creative_id,date,impressions,in_view_time
        )
            
            
        
@pipeline.task(depends_on=parse_log)

#converts the list into a CSV file
def list_to_csv(lines):
    tempFile =open('temporary.csv','r+')
    def build_csv(lines, header=None, file=None):
        
        if header:
            lines = itertools.chain([header], lines)
        writer = csv.writer(file, delimiter=',')
        writer.writerows(lines)
        file.seek(0)
        return file
    
    return build_csv(lines, 
                     header=['account_id','campaign_id','creative_id','date','impressions_id','in_view_time'], 
                     file=tempFile)
@pipeline.task(depends_on=list_to_csv)

#calculates total number of view in time for acoount_id for a specific campaign_id along with total number of views
def perform_analysis(csv_file):  
    #reader = csv.reader(csv_file)
    df=pd.read_csv(csv_file)
    df=df.groupby(['account_id','campaign_id','creative_id','impressions_id','date'])['in_view_time'].agg(['sum','count','mean'])
    
    return df

@pipeline.task(depends_on=perform_analysis)
def df_to_tsv(df):
    file_created= df.to_csv('finalreport.txt','|')
    return file_created
log=open ('logLines.txt')

split_lines=pipeline.run(log)
print(split_lines)