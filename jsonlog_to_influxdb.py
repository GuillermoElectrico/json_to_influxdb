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
import json

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
                new_map = yaml.load(open(self.influx_yaml), Loader=yaml.FullLoader)
                self.influx_map = new_map['influxdb']
                self.influx_map_last_change = path.getmtime(self.influx_yaml)
            except Exception as e:
                log.warning('Failed to re-load influxDB map, going on with the old one.')
                log.warning(e)
        return self.influx_map
        
    def listdirs(self, folder):
        return [d for d in os.listdir(folder) if os.path.isdir(os.path.join(folder, d))]
       

    def read_and_store(self, pathlog_json, pathold_log, extension_log):
        influxdb = self.get_influxdb()
        
        print('Search dirs in path "{}"' .format(pathlog_json))
        print('Copy log files if processed in dir "{}"' .format(pathold_log))
        
        json_body = dict()
        
        #for base, dirs, files in os.walk(pathlog_json):
        
        dirs = self.listdirs(pathlog_json)
        
        for pathdir in dirs:
    
            print('Search log files in path "{}"' .format(pathlog_json +'/'+pathdir))
        
            filelist = 0
            list = 0
            
            for filename in os.listdir(pathlog_json +'/'+ pathdir):
                if filename.endswith(extension_log):
                    filelist = filelist + 1
                    
                    print('File processed "{}"' .format(filename))
                    
                    for influx_config in influxdb:
                    
                        print('Send to "{}" db server' .format(influx_config['name']))

                        DBclient = InfluxDBClient(influx_config['host'],
                                                influx_config['port'],
                                                influx_config['user'],
                                                influx_config['password'],
                                                influx_config['dbname'])
                    
                        with open(pathlog_json +'/'+ pathdir + '/' + filename, 'r') as file:
                            list = 0
                            for line in file:
                                list = list + 1
                #                log.debug(line)
                #                json_body = ast.literal_eval(line)
                                json_line = json.loads(line)
                                
                #                log.debug(json_line)
                                
                                for key in json_line.keys():
                                    try:
                                        json_line[key] = float(json_line[key])
                                    except ValueError:
                                        pass
                                
                                json_body = [
                                    {
                                        'measurement': json_line['h'],
                                        'tags': {
                                            'topic': json_line['t']
                                        },
                                        'time': json_line['d'],
                                        'fields': {
                                            'data':json_line['v']
                                        }
                                    }
                                ]
                                
                                if len(json_body) > 0:

                #                    log.debug(json_body)

                                    try:
                                        DBclient.write_points(json_body)
                                        log.info('Data written in "{}"' .format(influx_config['name']))
                                    except Exception as e:
                                        log.error('Data not written! in "{}"' .format(influx_config['name']))
                                        log.error(e)
                                        raise
                                else:
                                    log.warning('No data sent.')
                    print('Read %d records from file "{}".' .format(filename) % list )
                    
                    if not os.path.exists(pathlog_json +'/'+ pathdir +'/'+ pathold_log):
                        os.makedirs(pathlog_json +'/'+pathdir+'/'+pathold_log)
                    
                    shutil.move(pathlog_json +'/'+ pathdir + '/' + filename, pathlog_json +'/'+ pathdir +'/'+ pathold_log + "/" + filename)
                    
            print('Processed %d files from dir "{}".' .format(pathdir) % filelist )

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=sys.path[0], 
                        help='Path to containing files with extension definied . Default "same/dir/as/this/script"')
    parser.add_argument('--pathold', default=".old", 
                        help='Path to containing files processed . Default "same/dir/as/this/script/.old"')
    parser.add_argument('--extension', default=".log", 
                        help='Extension file to processed . Default ".log"')
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

    print('Started at {}' .format(datetime.now()))
    
    collector = DataCollector(influx_yaml=args.influxdb)
    
    collector.read_and_store(pathlog_json=args.path,pathold_log=args.pathold,extension_log=args.extension)

    print('End script at {}' .format(datetime.now()))

    


