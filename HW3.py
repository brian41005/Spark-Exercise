
# coding: utf-8

# # 103590450 四資三 馬茂源
# ## Programming Exercise: MapReduce
# * Goal: A MapReduce program for analyzing the check-in records
#
#         Input: Check-in records in social networking site foursquare
#         Check-in records: <user_id, venue_id, checkin_time>
#         Venues: <venue_id, category, latitude, longitude>
#
# * Output: analysis results (to be detailed later)
#

# In[1]:

# import findspark
# findspark.init()
import pyspark
import time
from operator import add
from datetime import datetime
from geopy.geocoders import Nominatim
sc = pyspark.SparkContext()
geolocator = Nominatim()


# In[2]:

def row_split(string):
    items = string.split(',')[1:]
    return (items[0], ' '.join(items[1:]))


# ## Load file

# In[3]:

checking_local_dedup = sc.textFile('4sq_data/checking_local_dedup.txt')
local_place = sc.textFile('4sq_data/local_place.txt')
venue_info = sc.textFile('4sq_data/venue_info.txt')


# ## Lists the top checked-in venues (most popular)

# In[4]:

ts = time.time()
checking_place_id = checking_local_dedup.map(lambda venues: (venues.split(
    ',')[1], 1))                                         .reduceByKey(lambda x, y: x + y)

place_name = local_place.map(row_split).reduceByKey(
    lambda x, y: ' '.join([x, y]))

d = checking_place_id.leftOuterJoin(place_name)
result1 = d.sortBy(lambda x: x[1][0], ascending=False)
print('time:%.2f' % (time.time() - ts))


# In[5]:

result1.take(10)


# ## Lists the most popular categories

# In[6]:

def row_split2(line):
    items = line.split(',')[:-2]
    return (items[0], ' '.join(items[1:]))


# In[7]:

ts = time.time()
categories = venue_info.map(row_split2)                         .distinct()                         .join(result1)                         .map(
    lambda item: (item[1][0], item[1][1][0]))                         .reduceByKey(add)                         .sortBy(lambda item: item[1], ascending=False)
print('time:%.2f' % (time.time() - ts))


# In[8]:

categories.take(10)


# ## Lists the top checked-in users

# In[9]:

ts = time.time()
user = checking_local_dedup.map(lambda line: (line.split(',')[0], 1))                              .reduceByKey(
    add)                              .sortBy(lambda item: item[1], ascending=False)
print('time:%.2f' % (time.time() - ts))


# In[10]:

user.take(10)


# ##  Lists the most popular time for check-ins
# ### (in time slots in hours, for example, 7:00-8:00 or 18:00-19:00)
#

# In[11]:

def convert_stamp(line):
    stamp = line.split(',')[-1]
    time = int(datetime.utcfromtimestamp(float(stamp)).strftime('%H'))
    return ('%2d:00~%2d:00' % (time, time + 1), 1)


# In[12]:

ts = time.time()
checkin_time = checking_local_dedup.map(convert_stamp)                                    .reduceByKey(
    add)                                    .sortBy(lambda item: item[1], ascending=False)
print('time:%.2f' % (time.time() - ts))


# In[13]:

checkin_time.take(20)


# ## Optional functions
# * Using other attributes: latitude, longitude, …

# In[14]:

def get_name(line):
    latitude, longitude = line.split(',')[-2:]
    return ('%s, %s' % (latitude, longitude), 1)


# In[15]:

ts = time.time()
countries = venue_info.map(get_name)                       .reduceByKey(
    add)                       .sortBy(lambda item: item[1], ascending=False)
print('time:%.2f' % (time.time() - ts))


# In[16]:

for item in countries.take(5):
    print('%s [%d]' % (geolocator.reverse(item[0]), item[1]))


# In[ ]:
