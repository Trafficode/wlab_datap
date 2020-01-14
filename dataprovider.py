#   ------------------------------------------------------------------------- /
#   wlab_web_dataprovider: dataprovider.py
#   Created on: 29 sie 2019
#   Author: Trafficode
#   ------------------------------------------------------------------------- /

import os
import json
import logging
import traceback
from datetime import datetime

logger = logging.getLogger(__name__)

class DataProvider(object):
    ''' DataProvider '''
    def __init__(self, _db_path):
        self.logger = logger
        self.dbPath = _db_path
    
    def stationRegister(self, _uid, _descriptor):
        uid_path = os.path.join(self.db_path, _uid)
        if not os.path.exists(uid_path):
            os.makedirs(uid_path)
        desc_f = open(os.path.join(uid_path, "desc.json"), "w")
        desc_f.write(json.dumps(_descriptor, indent=4))
        desc_f.close()
    
    def stationSampleStore(self, _sample):
        dt = datetime.fromtimestamp(_sample["TS"])
        year, month, day = dt.strftime("%Y-%m-%d").split("-")
        
        for serie in _sample["SERIE"]:
            dst_sample = os.path.join(self.db_path, _sample["UID"])
            dst_sample = os.path.join(dst_sample, serie)
            dst_sample = os.path.join(dst_sample, year)
            dst_sample = os.path.join(dst_sample, month)
            if not os.path.exists(dst_sample):
                os.makedirs(dst_sample)
            
            dst_sample_file = os.path.join(dst_sample, day + ".json")
            
            _serie = _sample["SERIE"][serie]
            if not os.path.exists(dst_sample_file):
                sample_json = {
                    "general":  {
                        "f_avg_buff": _serie["f_avg"],
                        "f_act": _serie["f_act"],
                        "i_act_ts": _sample["TS"],
                        "f_min": _serie["f_min"],
                        "i_min_ts": _serie["i_min_ts"],
                        "f_max": _serie["f_max"],
                        "i_max_ts": _serie["i_max_ts"],
                        "i_counter": 1
                    },
                    _sample["TS"]: _sample["SERIE"][serie]
                }
                sample_f = open(dst_sample_file, "w")
                sample_f.write(json.dumps(sample_json, indent=4))
                sample_f.close()
            else:
                sample_f = open(dst_sample_file, "r")
                samples_dict = json.load(sample_f)
                sample_f.close()
                
                samples_dict[_sample["TS"]] = _serie
                
                if samples_dict["general"]["f_min"] > _serie["f_min"]:
                    samples_dict["general"]["f_min"] = _serie["f_min"]
                    samples_dict["general"]["i_min_ts"] = _serie["i_min_ts"]
                    
                if samples_dict["general"]["f_max"] < _serie["f_max"]:
                    samples_dict["general"]["f_max"] = _serie["f_max"]
                    samples_dict["general"]["i_max_ts"] = _serie["i_max_ts"]
                
                samples_dict["general"]["f_act"] = _serie["f_act"]
                samples_dict["general"]["i_act_ts"] = _sample["TS"]
                
                samples_dict["general"]["f_avg_buff"] += _serie["f_avg"]
                samples_dict["general"]["i_counter"] += 1
                
                sample_f = open(dst_sample_file, "w")
                sample_f.write(json.dumps(samples_dict, indent=4))
                sample_f.close()
    
    def getSerieMonthlyData(self, _uid, _serie, _date_year_month):
        splited_date = _date_year_month.split("-")
        return self.__serie_get_monthly(_uid, _serie, 
                                        splited_date[0], splited_date[1])
    
    def getSerieYearlyData(self, _uid, _serie, _date_year):
        yearly_data = {}
        year_path = os.path.join(self.dbPath, _uid)
        year_path = os.path.join(year_path, _serie)
        year_path = os.path.join(year_path, _date_year)
        
        if os.path.exists(year_path):
            yearly_months = list(os.listdir(year_path))
            self.logger.debug("yearly_months: %s" % str(yearly_months))
            for _month in yearly_months:
                yearly_data[_month] = self.__serie_get_monthly_general(
                    _uid, _serie, _date_year, _month)
        return yearly_data
        
    def getSerieDayData(self, _uid, _serie, _date):
        _result_serie_day = {}
        _year, _month, _day = _date.split("-")
        src_path = os.path.join(self.dbPath, _uid)
        src_path = os.path.join(src_path, _serie)
        src_path = os.path.join(src_path, _year)
        src_path = os.path.join(src_path, _month)
        src_path = os.path.join(src_path, _day+".json")
        self.logger.info("src_path: %s" % src_path)
        if os.path.exists(src_path):
            try:
                _day_f = open(src_path)
                _result_serie_day = json.load(_day_f)
                _day_f.close()
            except:
                self.logger.exception(
                    "Exception while getSerieDayData():\n%s" %\
                    traceback.format_exc())
        else:
            self.logger.error("path doesnt exist %s" % src_path)
        return _result_serie_day
            
    def getStationsDateTree(self):
        stations_data_tree = {} 
        stations_list = list(os.listdir(self.dbPath))
        for station in stations_list:
            stations_data_tree[station] = {}
            station_series_path = os.path.join(self.dbPath, station)
            station_series = list(os.listdir(station_series_path))
            if "desc.json" in station_series:
                station_series.remove("desc.json")
            for serie in station_series:
                stations_data_tree[station][serie] = {}
                station_serie_path = os.path.join(station_series_path, serie)
                station_serie_years = list(os.listdir(station_serie_path))
                stations_data_tree[station][serie]["years"] = \
                    station_serie_years
                for _year in station_serie_years:
                    stations_data_tree[station][serie][_year] = {}
                    station_serie_year_path = \
                                    os.path.join(station_serie_path, _year)
                    station_serie_year_months = \
                                    list(os.listdir(station_serie_year_path))
                    stations_data_tree[station][serie][_year]["months"] = \
                                    station_serie_year_months
                    for _month in station_serie_year_months:
                        stations_data_tree[station][serie][_year][_month] = {}
                        station_serie_year_month_path = \
                            os.path.join(station_serie_year_path, _month)
                        station_serie_year_month_days = \
                            list(os.listdir(station_serie_year_month_path))
                        days_list = []
                        for _day in station_serie_year_month_days:
                            days_list.append(_day.split(".")[0])
                        
                        stations_data_tree[station][serie][_year][_month] = \
                            days_list
        return stations_data_tree
                            
    def getStationsDesc(self):
        self.logger.debug("getStationsDesc()")
        stations_desc = {}
        
        if not os.path.exists(self.dbPath):
            self.logger.error("database path doesnt exist")
            return {}
        
        stations_list = sorted(list(os.listdir(self.dbPath)))
        if stations_list:
            for station in stations_list:
                station_desc_path = os.path.join(self.dbPath, station)
                station_desc_path = os.path.join(station_desc_path,"desc.json")
                if os.path.exists(station_desc_path):
                    station_desc_f = open(station_desc_path, "r")
                    stations_desc[station] = json.load(station_desc_f)
                    station_desc_f.close()
        else:
            return {}
        return stations_desc
    
    def getStationsNewest(self):
        self.logger.debug("getStationsNewest()")
        if not os.path.exists(self.dbPath):
            self.logger.error("database path doesnt exist")
            return {}
        
        series_newest = {}
        
        stations_list = sorted(list(os.listdir(self.dbPath)))
        if stations_list:
            for station in stations_list:
                station_path = os.path.join(self.dbPath, station)
                series_list = list(os.listdir(station_path))
                self.logger.debug("serie_list: %s" % str(series_list))
                if "desc.json" in series_list:
                    series_list.remove("desc.json")                    
                    series_newest[station] = {}
                    for serie in series_list:
                        self.logger.debug("found serie: " + str(serie))
                        series_newest[station][serie] = {}
                        station_serie_path = os.path.join(station_path, serie)
                        station_serie_newest = \
                                self.__serie_get_newest_day(station_serie_path)
                        if station_serie_newest:
                            if "general" in station_serie_newest:
                                series_newest[station][serie] = \
                                    station_serie_newest["general"]
                            else:
                                self.logger.error("no general in newest %s" \
                                                  % station_serie_path
                                              )
        return series_newest
    
    def __serie_get_newest_day(self, _serie_path):
        year_list_sorted = sorted(list(os.listdir(_serie_path)))
        self.logger.debug("year_list_sorted %s" % year_list_sorted)
        if len(year_list_sorted):
            newest_year = year_list_sorted[len(year_list_sorted)-1]
            self.logger.debug("newest_year %s" % newest_year)
        else:
            return None
        
        newest_year_path = os.path.join(_serie_path, newest_year)

        month_list_sorted = sorted(list(os.listdir(newest_year_path)))
        self.logger.debug("month_list_sorted %s" % month_list_sorted)
        if len(month_list_sorted):
            newest_month = month_list_sorted[len(month_list_sorted)-1]
            self.logger.debug("newest_month %s" % newest_month)
        else:
            return None
        
        newest_month_path = os.path.join(newest_year_path, newest_month)
        
        day_list_sorted = sorted(list(os.listdir(newest_month_path)))
        self.logger.debug("day_list_sorted %s" % day_list_sorted)
        if len(day_list_sorted):
            newest_day = day_list_sorted[len(day_list_sorted)-1]
            self.logger.debug("newest_day %s" % newest_day)
        else:
            return None
        
        newest_day_path = os.path.join(newest_month_path, newest_day)
        try:
            newest_day_f = open(newest_day_path, "r")
            newest_day_data = json.load(newest_day_f)
            newest_day_f.close()
        except:
            self.logger.exception(traceback.format_exc())
            return None
        
        return newest_day_data
        
    def __serie_get_monthly(self, _station_uid, _station_serie, _year, _month):
        station_mont_data = {}
        month_path = os.path.join(self.dbPath, _station_uid)
        month_path = os.path.join(month_path, _station_serie)
        month_path = os.path.join(month_path, _year)
        month_path = os.path.join(month_path, _month)
        
        if os.path.exists(month_path):
            day_list = list(os.listdir(month_path))
            self.logger.debug("day_list: %s" % str(day_list))
            for _day in day_list:
                _day_data_path = os.path.join(month_path, _day)
                _day_data_f = open(_day_data_path)
                _daily_general = json.load(_day_data_f)["general"]
                _day_data_f.close()
                station_mont_data[_day.split(".")[0]] = _daily_general
                
        return station_mont_data
        
    def __serie_get_monthly_general(self, _station_uid, _station_serie, _year, _month):
        monthly_general = {
            "f_min": 99999.0, 
            "i_min_ts": 0, 
            "f_max": -99999.0, 
            "i_max_ts": 0, 
            "f_avg": 0.0
        }
        
        month_path = os.path.join(self.dbPath, _station_uid)
        month_path = os.path.join(month_path, _station_serie)
        month_path = os.path.join(month_path, _year)
        month_path = os.path.join(month_path, _month)
        
        if os.path.exists(month_path):
            day_list = list(os.listdir(month_path))
            self.logger.debug("day_list: %s" % str(day_list))

            avg_buff = 0.0
            avg_buff_counter = 0
            
            for _day in day_list:
                _day_data_path = os.path.join(month_path, _day)
                _day_data_f = open(_day_data_path)
                _daily_general = json.load(_day_data_f)["general"]
                _day_data_f.close()
                
                avg_buff += _daily_general["f_avg_buff"]/_daily_general["i_counter"]
                avg_buff_counter += 1
                
                if monthly_general["f_min"] > _daily_general["f_min"]:
                    monthly_general["f_min"] = _daily_general["f_min"]
                    monthly_general["i_min_ts"] = _daily_general["i_min_ts"]

                if monthly_general["f_max"] < _daily_general["f_max"]:
                    monthly_general["f_max"] = _daily_general["f_max"]
                    monthly_general["i_max_ts"] = _daily_general["i_max_ts"]
            
            monthly_general["f_avg"] = avg_buff/avg_buff_counter
        
        return monthly_general
    
#   ------------------------------------------------------------------------- /
#    end of file
#   ------------------------------------------------------------------------- /
    