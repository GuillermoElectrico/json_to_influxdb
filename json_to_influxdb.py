#!/usr/bin/env python3

from influxdb import InfluxDBClient
from datetime import datetime, timedelta
from os import path
import sys
import os
import time
import yaml
import logging
import subprocess
import ast

# Change working dir to the same dir as this script
os.chdir(sys.path[0])

class DataCollector:
    def __init__(self, influx_yaml):
        self.influx_yaml = influx_yaml
        self.influx_map = None
        self.influx_map_last_change = -1
        log.info('InfluxDB:')
        for influx_config in sorted(self.get_influxdb(), key=lambda x:sorted(x.keys())):
            log.info('\t {} <--> {}'.format(influx_config['host'], influx_config['name']))

    def get_influxdb(self):
        assert path.exists(self.influx_yaml), 'InfluxDB map not found: %s' % self.influx_yaml
        if path.getmtime(self.influx_yaml) != self.influx_map_last_change:
            try:
                log.info('Reloading influxDB map as file changed')
                new_map = yaml.load(open(self.influx_yaml))
                self.influx_map = new_map['influxdb']
                self.influx_map_last_change = path.getmtime(self.influx_yaml)
            except Exception as e:
                log.warning('Failed to re-load influxDB map, going on with the old one.')
                log.warning(e)
        return self.influx_map
       

    def read_and_store(self, filelog_json):
        influxdb = self.get_influxdb()
        
        log.info('Open file {}' .format(filelog_json))
        
        json_body = dict()
        
        with open(filelog_json, 'r') as file:
            list = 0
            for line in file:
                list = list + 1
#                log.debug(line)
                json_body = ast.literal_eval(line)
    
                if len(json_body) > 0:

 #                   log.debug(json_body)

                    for influx_config in influxdb:

                        DBclient = InfluxDBClient(influx_config['host'],
                                                influx_config['port'],
                                                influx_config['user'],
                                                influx_config['password'],
                                                influx_config['dbname'])
                        try:
                            DBclient.write_points(json_body)
                            log.info('Data written in {}' .format(influx_config['name']))
                        except Exception as e:
                            log.error('Data not written! in {}' .format(influx_config['name']))
                            log.error(e)
                            raise
                else:
                    log.warning('No data sent.')
            log.info('Read %d records from file.' % list )
            

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--filelog', default="log00000.log", 
                        help='file containing log in json format. Default "log00000.log"')
    parser.add_argument('--influxdb', default='influx_config.yml',
                        help='YAML file containing Influx Host, port, user etc. Default "influx_config.yml"')
    parser.add_argument('--log', default='CRITICAL',
                        help='Log levels, DEBUG, INFO, WARNING, ERROR or CRITICAL')
    parser.add_argument('--logfile', default='',
                        help='Specify log file, if not specified the log is streamed to console')
    args = parser.parse_args()
    loglevel = args.log.upper()
    logfile = args.logfile

    # Setup logging
    log = logging.getLogger('mqtt2influx-logger')
    log.setLevel(getattr(logging, loglevel))

    if logfile:
        loghandle = logging.FileHandler(logfile, 'w')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        loghandle.setFormatter(formatter)
    else:
        loghandle = logging.StreamHandler()

    log.addHandler(loghandle)

    log.info('Started app')
    
    collector = DataCollector(influx_yaml=args.influxdb)
    
    collector.read_and_store(filelog_json=args.filelog)

    log.info('End app')

    


