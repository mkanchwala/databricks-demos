# installWhlLibrary.py
#!/usr/bin/python3
import json
import requests
import sys
import getopt
import time
import os

def main():
  shard = ''
  token = ''
  clusterid = ''
  libspath = ''
  dbfspath = ''

  try:
    opts, args = getopt.getopt(sys.argv[1:], 'hstcld',
      ['shard=', 'token=', 'clusterid=', 'libs=', 'dbfspath='])
  except getopt.GetoptError:
    print(
      'installWhlLibrary.py -s <shard> -t <token> -c <clusterid> -l <libs> -d <dbfspath>')
    sys.exit(2)

  for opt, arg in opts:
    if opt == '-h':
      print(
        'installWhlLibrary.py -s <shard> -t <token> -c <clusterid> -l <libs> -d <dbfspath>')
      sys.exit()
    elif opt in ('-s', '--shard'):
      shard = arg
    elif opt in ('-t', '--token'):
      token = arg
    elif opt in ('-c', '--clusterid'):
      clusterid = arg
    elif opt in ('-l', '--libs'):
      libspath=arg
    elif opt in ('-d', '--dbfspath'):
      dbfspath=arg

  print('-s is ' + shard)
  print('-t is ' + token)
  print('-c is ' + clusterid)
  print('-l is ' + libspath)
  print('-d is ' + dbfspath)

  # Generate the list of files from walking the local path.
  libslist = []
  for path, subdirs, files in os.walk(libspath):
    for name in files:

      name, file_extension = os.path.splitext(name)
      if file_extension.lower() in ['.whl']:
        print('Adding ' + name + file_extension.lower() + ' to the list of .whl files to evaluate.')
        libslist.append(name + file_extension.lower())

  for lib in libslist:
    dbfslib = 'dbfs:' + dbfspath + '/' + lib
    print('Evaluating whether ' + dbfslib + ' must be installed, or uninstalled and reinstalled.')

    if (getLibStatus(shard, token, clusterid, dbfslib)) is not None:
      print(dbfslib + ' status: ' + getLibStatus(shard, token, clusterid, dbfslib))
      if (getLibStatus(shard, token, clusterid, dbfslib)) == "not found":
        print(dbfslib + ' not found. Installing.')
        installLib(shard, token, clusterid, dbfslib)
      else:
        print(dbfslib + ' found. Uninstalling.')
        uninstallLib(shard, token, clusterid, dbfslib)
        print("Restarting cluster: " + clusterid)
        restartCluster(shard, token, clusterid)
        print('Installing ' + dbfslib + '.')
        installLib(shard, token, clusterid, dbfslib)

def uninstallLib(shard, token, clusterid, dbfslib):
  values = {'cluster_id': clusterid, 'libraries': [{'whl': dbfslib}]}
  requests.post(shard + '/api/2.0/libraries/uninstall', data=json.dumps(values), auth=("token", token))

def restartCluster(shard, token, clusterid):
  values = {'cluster_id': clusterid}
  requests.post(shard + '/api/2.0/clusters/restart', data=json.dumps(values), auth=("token", token))

  waiting = True
  p = 0
  while waiting:
    time.sleep(30)
    clusterresp = requests.get(shard + '/api/2.0/clusters/get?cluster_id=' + clusterid,
      auth=("token", token))
    clusterjson = clusterresp.text
    jsonout = json.loads(clusterjson)
    current_state = jsonout['state']
    print(clusterid + " state: " + current_state)
    if current_state in ['TERMINATED', 'RUNNING','INTERNAL_ERROR', 'SKIPPED'] or p >= 10:
      break
      p = p + 1

def installLib(shard, token, clusterid, dbfslib):
  values = {'cluster_id': clusterid, 'libraries': [{'whl': dbfslib}]}
  requests.post(shard + '/api/2.0/libraries/install', data=json.dumps(values), auth=("token", token))

def getLibStatus(shard, token, clusterid, dbfslib):

  resp = requests.get(shard + '/api/2.0/libraries/cluster-status?cluster_id='+ clusterid, auth=("token", token))
  libjson = resp.text
  d = json.loads(libjson)
  if (d.get('library_statuses')):
    statuses = d['library_statuses']

    for status in statuses:
      if (status['library'].get('whl')):
        if (status['library']['whl'] == dbfslib):
          return status['status']
  else:
    # No libraries found.
    return "not found"

if __name__ == '__main__':
  main()