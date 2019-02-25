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
import shutil

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
       

    def read_and_store(self, pathlog_json, pathold_log):
        influxdb = self.get_influxdb()
        
        log.info('Find log files in path {}' .format(pathlog_json))
        log.info('Copy log files Processed in path {}' .format(pathold_log))
        
        json_body = dict()
        
        filelist = 0
        
        for filename in os.listdir(pathlog_json):
            if filename.endswith('.log'):
                filelist = filelist + 1
                
                log.info('File Processed {}' .format(filename))
                
                with open(filename, 'r') as file:
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
                    print('Read %d records from file {}.' .format(filename) % list )
                
                if not os.path.exists(pathold_log):
                    os.makedirs(pathold_log)
                
                shutil.move(filename, pathold_log + "/" + filename)
                
        print('Processed %d files from dir {}.' .format(pathlog_json) % filelist )

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=sys.path[0], 
                        help='Path to containing files with extension .log . Default "same/dir/as/this/script"')
    parser.add_argument('--pathold', default=sys.path[0]+"/.old", 
                        help='Path to containing files Processed . Default "same/dir/as/this/script/.old"')
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

    print('Started app')
    
    collector = DataCollector(influx_yaml=args.influxdb)
    
    collector.read_and_store(pathlog_json=args.path,pathold_log=args.pathold)

    print('End app')

    


