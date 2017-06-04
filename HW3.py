
# coding: utf-8

import pyspark
import time
from operator import add
from datetime import datetime
from geopy.geocoders import Nominatim
APP_NAME = "BDA-HW3 103590450"
conf = pyspark.SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc = pyspark.SparkContext()
geolocator = Nominatim()


def row_split(string):
    items = string.split(',')[1:]
    return (items[0], ' '.join(items[1:]))


def row_split2(line):
    items = line.split(',')[:-2]
    return (items[0], ' '.join(items[1:]))


def convert_stamp(line):
    stamp = line.split(',')[-1]
    time = int(datetime.utcfromtimestamp(float(stamp)).strftime('%H'))
    return ('%2d:00~%2d:00' % (time, time + 1), 1)


def get_name(line):
    latitude, longitude = line.split(',')[-2:]
    return ('%s, %s' % (latitude, longitude), 1)


if __name__ == '__main__':
    text_file = open("Output.txt", "w")
    output = ''
    line = '#' * 100 + '\n'
    checking_local_dedup = sc.textFile('4sq_data/checking_local_dedup.txt')
    local_place = sc.textFile('4sq_data/local_place.txt')
    venue_info = sc.textFile('4sq_data/venue_info.txt')

    ts = time.time()
    checking_place_id = checking_local_dedup.map(lambda venues: (venues.split(
        ',')[1], 1)).reduceByKey(lambda x, y: x + y)

    place_name = local_place.map(row_split).reduceByKey(
        lambda x, y: ' '.join([x, y]))

    d = checking_place_id.leftOuterJoin(place_name)
    result1 = d.sortBy(lambda x: x[1][0], ascending=False)

    output += 'Lists the top checked-in venues (most popular):\n'
    output += 'time:%.2f\n' % (time.time() - ts)
    for i in result1.take(10):
        output += str(i) + '\n'
    output += line

    ts = time.time()
    categories = venue_info.map(row_split2).distinct().join(result1).map(lambda item: (
        item[1][0], item[1][1][0])).reduceByKey(add).sortBy(lambda item: item[1], ascending=False)

    output += 'Lists the most popular categories:\n'
    output += 'time:%.2f\n' % (time.time() - ts)
    for i in categories.take(10):
        output += str(i) + '\n'
    output += line

    ts = time.time()
    user = checking_local_dedup.map(lambda line: (line.split(',')[0], 1)).reduceByKey(
        add).sortBy(lambda item: item[1], ascending=False)
    output += 'Lists the top checked-in users:\n'
    output += 'time:%.2f\n' % (time.time() - ts)
    for i in user.take(10):
        output += str(i) + '\n'
    output += line

    ts = time.time()
    checkin_time = checking_local_dedup.map(convert_stamp).reduceByKey(
        add).sortBy(lambda item: item[1], ascending=False)
    output += 'Lists the most popular time for check-ins:\n'
    output += 'time:%.2f\n' % (time.time() - ts)
    for i in checkin_time.take(20):
        output += str(i) + '\n'
    output += line

    ts = time.time()
    countries = venue_info.map(get_name).reduceByKey(
        add).sortBy(lambda item: item[1], ascending=False)

    output += 'Optional functions:\n'
    output += 'time:%.2f\n' % (time.time() - ts)
    for item in countries.take(5):
        output += '%s [%d]\n' % (geolocator.reverse(item[0]), item[1])
    print(output, file=text_file)
    text_file.close()
