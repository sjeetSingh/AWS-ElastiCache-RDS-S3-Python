import boto
import time
import mysql.connector
import csv
import StringIO
import random
import memcache
# STEP 1. ACCESS to AWS
# AWS credentials
AWSAccessKeyId=''
AWSSecretKey=''

# Connects to AWS using the above credentials
s3 = boto.connect_s3(AWSAccessKeyId, AWSSecretKey)

# Creates a new bucket
#bucket_name = raw_input('Please enter a bucket name: ')
bucket = s3.create_bucket('bucket1simran')
print 'Bucket has been created!'



#
key = bucket.new_key('sample.csv')              # EMPTY FILE made in SE 3 bucket
start = time.time()
key.set_contents_from_filename('my_data_file.csv')
key.set_acl('public-read')                      # Public read for all-user read.
print 'File uploaded!'
end = time.time()
local_to_AWS = end - start
print 'Time taken to upload is: '
print local_to_AWS

#Connect to RDS
dbObj = mysql.connector.connect(
    user='my_username',
    password='my_pwd',
    host='rds_instance',
    port='3306',
    db='myDB1',
    buffered = True
)

# LOCALHOST ADDRESS or EC2 instance address???
memc = memcache.Client(['Elastic_cache_endpoint'], debug=1)

print 'The connection has been created'

#Uploading data from S3 to RDS
try:
dbObj = mysql.connector.connect(
    user='my_username',
    password='my_pwd',
    host='rds_instance',
    port='3306',
    db='myDB1',
    buffered = True
)
    cursor = dbObj.cursor()
except:
    print 'Exception found: cannot connect to the Database'
reader = csv.reader(StringIO.StringIO(key.get_contents_as_string()), csv.excel)

data = []
for i in reader:
    data.append(i)
del data[0]

# for i in data:
    #print i
    #cursor.execute("""INSERT INTO dataTable (time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated, place, type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], i[13], i[14] ) )

# dbObj.commit()
# cursor.close()
# dbObj.close()
# print 'Done'

#print 'Time taken to put data from AWS Storage into RDS Instance is: '+str(time_taken)+' seconds.'


queryList_1000= []

queryList_1000.append("SELECT * FROM dataTable WHERE mag<=2 LIMIT 1000")
queryList_1000.append("SELECT * FROM dataTable WHERE place like 'cal%' LIMIT 1000")
queryList_1000.append("SELECT * FROM dataTable WHERE depth>5 LIMIT 1000")

queryList_5000= []

queryList_5000.append("SELECT * FROM dataTable WHERE mag>5 LIMIT 5000")
queryList_5000.append("SELECT * FROM dataTable WHERE horizontalError<1.6 LIMIT 5000")
queryList_5000.append("SELECT * FROM dataTable WHERE depth<5 LIMIT 5000")

queryList_10000= []

queryList_10000.append("SELECT * FROM dataTable WHERE mag>5 LIMIT 8000")
queryList_10000.append("SELECT * FROM dataTable WHERE depthError >3.5 LIMIT 8000")
queryList_10000.append("SELECT * FROM dataTable WHERE depth<5 LIMIT 8000")

# for i in range(1, 1000):
#     randomx = (1 , 8000)
#

#----------------------------------1000 random queries----------------------------------

try:
    memc_1_1000 = memc.get('top1000queries_1')
    memc_2_1000 = memc.get('top1000queries_2')
    memc_3_1000 = memc.get('top1000queries_3')
except:
    print 'This gave me an error'
print '1000 Random Queries'
queryST = time.time()

i=0
while i<10:
    rand_num = random.randint(0,2)
    if rand_num==0 and memc_1_1000:
        print '1000 queries # 1 found in memcached!'
        break
    elif rand_num==1 and memc_2_1000:
        print '1000 queries # 2 found in memcached!'
        break
    elif rand_num==2 and memc_3_1000:
        print '1000 queries # 3 found in memcached!'
        break
    else:
        cursor.execute(queryList_1000[rand_num])
        rows = cursor.fetchall()
        if rand_num==0:
            memc.set('top1000queries_1', rows, 0)
        elif rand_num==1:
            memc.set('top1000queries_2', rows, 0)
        elif rand_num==2:
            memc.set('top1000queries_3', rows, 0)
    i=i+1
queryET = time.time()
time_taken = queryET - queryST
print 'Time taken is ' + str(time_taken) +'s to run 1000 queries.'

#----------------------------------5000 random queries----------------------------------

memc_1_5000 = memc.get('to5000queries_1')
memc_2_5000 = memc.get('top5000queries_2')
memc_3_5000 = memc.get('top5000queries_3')

print '5000 Random Queries'
queryST = time.time()
i=0
while i<10:
    rand_num = random.randint(0,2)
    if rand_num==0 and memc_1_5000:
        print '5000 queries # 1 found in memcached!'
        break
    elif rand_num==1 and memc_2_5000:
        print '5000 queries # 2 found in memcached!'
        break
    elif rand_num==2 and memc_3_5000:
        print '5000 queries # 3 found in memcached!'
        break
    else:
        cursor.execute(queryList_5000[rand_num])
        rows = cursor.fetchall()
        if rand_num==0:
            memc.set('top5000queries_1', rows, 0)
        elif rand_num==1:
            memc.set('top5000queries_2', rows, 0)
        elif rand_num==2:
            memc.set('top5000queries_3', rows, 0)
    i=i+1
print 'Value of memc_1_5000 is: '+str(memc_1_5000)
print 'Value of memc_2_5000 is: '+str(memc_2_5000)
print 'Value of memc_3_5000 is: '+str(memc_3_5000)
queryET = time.time()
time_taken = queryET - queryST
print 'Time taken is ' + str(time_taken) +'s to run 5000 queries.'

#----------------------------------10000 random queries----------------------------------

memc_1_10000 = memc.get('top10000queries_1')
memc_2_10000 = memc.get('top10000queries_2')
memc_3_10000 = memc.get('top10000queries_3')

print '10000 Random Queries'
queryST = time.time()
i=0
while i<10:
    rand_num = random.randint(0,2)
    if rand_num==0 and memc_1_10000:
        print '5000 queries # 1 found in memcached!'
        break
    elif rand_num==1 and memc_2_10000:
        print '5000 queries # 2 found in memcached!'
        break
    elif rand_num==2 and memc_3_10000:
        print '5000 queries # 3 found in memcached!'
        break
    else:
        cursor.execute(queryList_10000[rand_num])
        rows = cursor.fetchall()
        if rand_num==0:
            memc.set('top10000queries_1', rows, 0)
        elif rand_num==1:
            memc.set('top10000queries_2', rows, 0)
        elif rand_num==2:
            memc.set('top10000queries_3', rows, 0)
    i=i+1

queryET = time.time()
time_taken = queryET - queryST
print 'Time taken is ' + str(time_taken) +'s to run 10000 queries.'
