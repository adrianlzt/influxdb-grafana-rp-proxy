"""
Auto Select RetentionPolicy InfluxDB 0.10 proxy for grafana
Authors: Zollner Robert,
         Paul Kuiper

Free use
Requires: gevent, bottle, requests
"""

import gevent
from gevent import monkey
from optparse import OptionParser

monkey.patch_all()

import sys
import requests
from bottle import get, abort, run, request, response, redirect
import regex as re

import logging
logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.CRITICAL)

rp_db_map = dict()

CONFIG = {
    'influxdb_http':'http://localhost:8086',

    'bind_host': '0.0.0.0',
    'bind_port': '3004',

    'retention_policy_map' : {
        '0.1s': '"default"',
        '1s' : '"default"',
        '5s' : '"default"',
        '10s': '"rp_10s"',
        '30s': '"rp_30s"',
        '1m' : '"rp_1m"',
        '5m' : '"rp_5m"',
        '10m': '"rp_30m"',
        '30m': '"rp_30m"',
        '1h' : '"rp_1h"',
        '3h' : '"rp_3h"',
        '12h': '"rp_12h"',
        '1d' : '"rp_24h"',
        '7d' : '"rp_24h"',
        '30d': '"rp_24h"'
    }
}

pattern = re.compile(r"""

    ^                   # beginning of string
    select\b            # must start with select statement (followed by word boundary)
    \s+                 # 1 or more whitespaces
    (count|min|max|mean|sum|first|last) # an aggragate function                         group 0 (aggregate)
    \(                  # time with opening bracket
    (.*)                # the field name                                                group 1 (field name)
    \)                  # closing bracket
    \s+                 # 1 or more whitespaces
    \bfrom\b            # the from statement should follow (with word boundaries)
    \s+                 # 1 or more whitespaces
    (.*)                # the from content                                              group 2  (measurement)
    \s+                 # 1 or more whitespaces
    \bwhere\b           # the where statement is always present in a grafana query
    (.*)                # the where content                                             group 3  (where clause)
    \bgroup\sby\b       # match group by statement
    \s+                 # 1 or more whitespaces
    time\(              # time with opening bracket
    (\d+)               # minimal 1 digit (does not match 0.1s!)                        group 4  (number of time units)
    ([s|m|h|d|w])       # the group by unit                                             group 5  (time unit)
    \)                  # closing bracket
    .*                  # rest of the request - don't care
    $                   # end of string
    """,  re.VERBOSE | re.I)


@get('/<path:path>')
def proxy_influx_query(path):
    """
    Capture the query events comming from Grafana.
    Investigate the query and replace the measurement name with a Retention Policy measurement name if possible.
    Send out the (modified or unmodified) query to Influx and return the result
    """

    forward_url = CONFIG['influxdb_http']  # The local influx host

    params = dict(request.query) # get all query parameters

    auth = request.auth
    logger.info("Original query:    %s", params['q'])

    try:
        params['q'] = modify_query(params, rp_db_map, auth)

    except Exception as e:
        logger.critical("Exception in proxy_influx_query():")
        logger.exception(e)
        pass

    headers = request.headers
    cookies = request.cookies

    logger.debug("Peticion hacia el servidor: ")
    for k in headers.keys():
        logger.debug("headers: %s -> %s", k, headers.raw(k))
    logger.debug("cookies: %s", cookies)
    logger.debug("url: %s", forward_url +'/'+ path)
    logger.info("Transformed query: %s", params['q'])

    s = requests.Session()
    req = requests.Request('GET', forward_url +'/'+ path, params=params, headers=headers, cookies=cookies)
    prepped = req.prepare()
    r = s.send(prepped, stream=True)

    # Do now try to read response with r.content or r.raw.data because it will close the file (r.raw)
    logger.debug("influx response code: %s", r.status_code)

    if r.status_code == 200:
        for key, value in dict(r.headers).iteritems():
            response.set_header(key, value)

        for key, value in dict(r.cookies).iteritems():
            response.cookies[key] = value
    else:
        abort(r.status_code, r.content) # NOK, return error

    logger.debug("Return response headers: %s", r.headers)
    logger.debug("Return response cookies: %s", r.cookies)
    return r.raw


def modify_query(req, rp_db_map, auth):

    """
    Grafana will zoom out with the following group by times:
    0.1s, 1s, 5s, 10s, 30s, 1m, 5m, 10m, 30m, 1h, 3h, 12h, 1d, 7d, 30d, 1y
    """

    qry = req['q']
    qry_db = req['db']

    logger.debug("modify_query() with db=%s and query: %s", qry_db, qry)

    try:
        pattern_qry = pattern.search(qry)
        if pattern_qry is None:
            return qry

        items = pattern_qry.groups() # get the content of the different query parts

        q_gtime = ''.join((items[4],items[5]))
        logger.debug("Original group by time: %s", q_gtime)
        if q_gtime not in CONFIG['retention_policy_map']:
            logger.warn("Group by time not found in the CONFIG['retention_policy_map']")
            return qry
            
        q_table = items[2]
        logger.debug("Original measurement queried: %s", q_table)
        if '.' in q_table:
            q_rp,_,q_table = items[2].partition('.')
            if q_rp in CONFIG['retention_policy_map'].values():
                logger.info('specific RP requested, ignoring detection: %s - %s', q_rp, q_table)
                return qry
            else:
                # This is a dotted series name
                q_table = items[2]
         
                
            logger.info("Dot founds. After transform: Group by time: %s. RP: %s. Measurement: %s", q_gtime, q_rp, q_table)
            
        new_rp = CONFIG['retention_policy_map'][q_gtime]
        logger.debug("New RP: %s", new_rp)
        
        measurement = '.'.join((new_rp, q_table))
        new_qry = qry.replace(items[2], measurement)
        logger.debug("New query: %s", new_qry)
        
        # Download list of RP for current Influxdb database
        if qry_db not in rp_db_map:
            influx_update_rp(rp_db_map, qry_db, auth);
        
        # Check if auto-calc RP is defined in InfluxDB database
        if new_rp.strip("\"") not in rp_db_map.get(qry_db, []):
            logger.critical("RP [%s] in not defined in Influx database [%s]. skipping...", new_rp, qry_db)
            logger.critical("RPs available: %s", rp_db_map.get(qry_db, []))
            return qry
            
        logger.debug('original measurement :[%s] new measurement: [%s]', items[2], measurement)
        return new_qry
        
    except Exception as e:
        logger.critical("Exception in modify_query():")
        logger.exception(e)
        return qry


def influx_update_rp(rp_map, r_db, auth):
    
    params = {  'q' : 'SHOW RETENTION POLICIES ON %s' % r_db,
                'db' : r_db}

    try:
        r = requests.get(CONFIG['influxdb_http'] + '/query', params=params, auth=auth)

    except Exception as e:
        logger.critical("Exception in influx_update_rp():")
        logger.exception(e)
        pass

    if not r.ok:
        logger.warn("Error obtaining RPs from InfluxDB server: %s", r.content)
        return

    try:
        rp_list = { rp[0] for rp in r.json()['results'][0]['series'][0]['values'] }
        rp_map[r_db] = rp_list
        logger.debug("RPs for database %s: %s", r_db, rp_list)
        
    except Exception as e:
        logger.critical("Exception in influx_update_rp():")
        logger.exception(e)
        pass
        
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-v", action="store_true", dest="verbose")
    parser.add_option("-d", action="store_true", dest="debug")
    (options, args) = parser.parse_args(sys.argv)

    if options.verbose:
        logger.setLevel(logging.INFO)
        logger.info("Logger set to INFO level")
    if options.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Logger set to DEBUG level")

    print("Starting proxy server")
    run(host=CONFIG['bind_host'], port=CONFIG['bind_port'], server='gevent')
