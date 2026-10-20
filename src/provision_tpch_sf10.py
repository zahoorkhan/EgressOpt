#!/usr/bin/env python3
"""M6L: direct, authenticated SF10 Parquet placement on EXISTING research VMs.

From ~/Research/EgressOpt:
    python src/provision_tpch_sf10.py --mode preflight
    python src/provision_tpch_sf10.py --mode apply --ack-cloud-cost

NO object storage, NO Phase 1 changes, NO automatic SF10 query execution.
Streams each Parquet file Mac -> pinned-TLS temporary service on assigned VM.
Each file is verified by SHA256 + DuckDB COUNT(*) before atomic installation.
Uses q05.py's existing AWS SSM, Azure Run Command and GCP IAP management.
"""
from __future__ import annotations

import argparse
import base64
import datetime
import gzip
import hashlib
import http.client
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import ssl
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
PHASE1_SHA256 = 'bc7ac734062e88e13e4a23b3b6368c7a5623c6a806adfef63c9d913462526d21'
LOCAL_ROOT = ROOT/'data'/'tpch'/'parquet_sf10'
DEST_ROOT = '/opt/egressopt/data/tpch_sf10'
PORT = 18443  # distinct from Phase 1/Phase 2 query worker 8443
FILE_LIST = {
    'aws': {'orders': (384676403, 15000000, 'f601a81423eeaa35766fed911e8d91a3d96181cbff4cb02341db1a9e9a25f3b1')},
    'azure': {
        'customer': (79830051, 1500000, 'f5852350d4079789eff053ea15097d0eecbd9cff41b471b40d444077defbd8f9'),
        'lineitem': (1565341081, 59986052, '72e123982814d18fe62c1ad45c014fd54d01bd3f8232517a242d82edc02da96e'),
        'nation': (2009, 25, '5279e1c8282a4b6a97258f575536b89cd3eecf09558bd02f61d4b02c141087c4'),
    },
    'gcp': {
        'supplier': (5111453, 100000, '3d7140b3ca705ed6b6bebb382d0b7436a0d2ff4c15244d085d096a3f0a3865ec'),
        'region': (997, 5, 'a68a52afe4fa7a2db0b791a6f1b43df5bcdaed0631bfcb8e6b8833ae0bd5f43e')
    }
}
REMOTE_WORKER_SOURCE = r'''#!/usr/bin/env python3
"""Temporary pinned-TLS, one-dataset, append-safe streaming receiver."""
import argparse,hashlib,hmac,json,os,re,ssl,uuid
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import duckdb
p=argparse.ArgumentParser()
p.add_argument('--cert',required=True);p.add_argument('--key',required=True)
p.add_argument('--manifest',required=True)
a=p.parse_args()
with open(a.manifest) as f: spec=json.load(f)
DATA=Path('/opt/egressopt/data/tpch_sf10')
TOKEN=os.environ['M6L_TOKEN']
ALLOWED=spec['files']
PORT=18443

def verify(name):
    path=DATA/(name+'.parquet')
    if not path.is_file():return {'state':'missing','table':name}
    expected=ALLOWED[name]
    size=path.stat().st_size
    if size!=expected['bytes']:
        return {'state':'mismatch','table':name,'reason':'size'}
    h=hashlib.sha256()
    with path.open('rb') as src:
        for block in iter(lambda:src.read(4*1024*1024),b''):h.update(block)
    if not hmac.compare_digest(h.hexdigest(),expected['sha256']):
        return {'state':'mismatch','table':name,'reason':'sha256'}
    with duckdb.connect(':memory:') as con:
        cnt=con.execute('SELECT COUNT(*) FROM read_parquet(?)',[str(path)]).fetchone()[0]
    if cnt!=expected['rows']:
        return {'state':'mismatch','table':name,'reason':'row_count','rows':cnt}
    return {'state':'verified','table':name,'bytes':size,'rows':cnt,'sha256':h.hexdigest()}

class Handler(BaseHTTPRequestHandler):
    server_version='EgressOptM6L/1.0'
    def log_message(self,*args):pass
    def send(self,status,data):
        b=json.dumps(data,sort_keys=True,separators=(',',':')).encode()
        self.send_response(status)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(b)))
        self.end_headers();self.wfile.write(b)
    def authorized(self):
        if not hmac.compare_digest(self.headers.get('Authorization',''),'Bearer '+TOKEN):
            self.send(401,{'error':'unauthorized'});return False
        return True
    def table(self,prefix):
        name=self.path[len(prefix):]
        if self.path.startswith(prefix) and re.fullmatch(r'[a-z]+',name) and name in ALLOWED:
            return name
        return None
    def do_GET(self):
        if not self.authorized():return
        try:
            if self.path=='/health':
                self.send(200,{'status':'ok','protocol':'M6L-HTTPS','site':spec['site'],'duckdb':duckdb.__version__});return
            name=self.table('/status/')
            if name:self.send(200,verify(name));return
            self.send(404,{'error':'not_found'})
        except Exception as e:self.send(422,{'error':type(e).__name__+': '+str(e)[:300]})
    def do_PUT(self):
        if not self.authorized():return
        name=self.table('/data/')
        if not name:self.send(404,{'error':'unknown_table'});return
        req=ALLOWED[name]
        n=self.headers.get('Content-Length','')
        if n!=str(req['bytes']) or not hmac.compare_digest(self.headers.get('X-Expected-SHA256',''),req['sha256']):
            self.send(400,{'error':'size_or_hash_not_as_manifest'});return
        dest=DATA/(name+'.parquet')
        if dest.exists():
            state=verify(name)
            if state['state']=='verified':self.send(200,state);return
            self.send(409,{'error':'destination_exists_but_invalid','table':name});return
        DATA.mkdir(parents=True,exist_ok=True)
        tmp=DATA/(name+'.m6l-'+uuid.uuid4().hex+'.part')
        self.connection.settimeout(180)
        remaining=req['bytes'];h=hashlib.sha256()
        try:
            with tmp.open('xb') as out:
                while remaining:
                    data=self.rfile.read(min(4*1024*1024,remaining))
                    if not data:raise IOError('truncated_input')
                    h.update(data);out.write(data);remaining-=len(data)
                out.flush();os.fsync(out.fileno())
            if not hmac.compare_digest(h.hexdigest(),req['sha256']):
                self.send(422,{'error':'sha256_mismatch','table':name});return
            with duckdb.connect(':memory:') as con:
                count=con.execute('SELECT COUNT(*) FROM read_parquet(?)',[str(tmp)]).fetchone()[0]
            if count!=req['rows']:
                self.send(422,{'error':'row_count_mismatch','table':name,'rows':count});return
            # Exclusive, never overwrite existing verified or nonverified data.
            if dest.exists():
                self.send(409,{'error':'destination_created_concurrently'});return
            os.link(tmp,dest)
            self.send(200,{'state':'verified','table':name,'bytes':req['bytes'],
                'rows':count,'sha256':h.hexdigest()})
        except Exception as e:self.send(422,{'error':type(e).__name__+': '+str(e)[:300],'table':name})
        finally:tmp.unlink(missing_ok=True)

srv=ThreadingHTTPServer(('0.0.0.0',PORT),Handler)
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.minimum_version=ssl.TLSVersion.TLSv1_2
ctx.load_cert_chain(a.cert,a.key)
srv.socket=ctx.wrap_socket(srv.socket,server_side=True)
srv.serve_forever()
'''


def sha_file(path: Path) -> str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
    return h.hexdigest()


def load_phase1():
    script=ROOT/'experiments'/'q05.py'
    if not script.exists() or sha_file(script)!=PHASE1_SHA256:
        raise RuntimeError('Frozen Phase 1 q05.py missing or changed; no cloud action permitted')
    spec=importlib.util.spec_from_file_location('egressopt_original_q05_cloud_only',script)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_manifest():
    path=ROOT/'results'/'dataset_validation'/'parquet_sf10_manifest.json'
    if not path.is_file():
        raise FileNotFoundError('Missing original SF10 conversion manifest: '+str(path))
    manifest=json.loads(path.read_text())
    if manifest.get('scale_factor')!=10:
        raise RuntimeError('Expected SF10 conversion manifest')
    for site,tables in FILE_LIST.items():
        for name,(size,rows,sha) in tables.items():
            ent=manifest['tables'].get(name,{})
            if (ent.get('bytes'),ent.get('rows'),ent.get('sha256'))!=(size,rows,sha):
                raise RuntimeError(f'SF10 manifest does not match locked {site}/{name} specification')
    return manifest


def preflight(verify_hashes=True):
    read_manifest()
    report={'mode':'M6L_LOCAL_SF10_PREFLIGHT','cloud_action':False,
            'phase1_code_changed':False,'provider_files':{},'total_bytes':0,'status':'CHECKING'}
    for site,tables in FILE_LIST.items():
        report['provider_files'][site]={}
        for name,(size,rows,sha) in tables.items():
            f=LOCAL_ROOT/(name+'.parquet')
            if not f.is_file() or f.stat().st_size!=size:
                raise RuntimeError(f'{site}/{name}: missing or unexpected size at {f}')
            if verify_hashes and sha_file(f)!=sha:
                raise RuntimeError(f'{site}/{name}: source SHA256 mismatch: {f}')
            report['provider_files'][site][name]={
                'source':str(f.relative_to(ROOT)), 'remote':DEST_ROOT+'/'+name+'.parquet',
                'bytes':size,'rows':rows,'sha256':sha,'source_sha256_verified':verify_hashes}
            report['total_bytes']+=size
    report['status']='PASS'
    return report


def run_cmd(infra,cmd):
    return infra.q05_cli(cmd,timeout=300)


def public_ipv4(s):
    address=ipaddress.ip_address(str(s).strip())
    if address.version!=4 or not address.is_global:
        raise ValueError('Expected globally routable public IPv4')
    return str(address)


def my_public_ip(explicit=None):
    if explicit:return public_ipv4(explicit)
    with urllib.request.urlopen('https://checkip.amazonaws.com',timeout=15) as response:
        return public_ipv4(response.read(70).decode().strip())


def remote_script(infra,site,command,label):
    return infra.q05_remote(site,command,'m6l-'+label)


def b64_marker(value,prefix):
    lines=re.findall(re.escape(prefix)+r'([A-Za-z0-9+/=]+)',value)
    if len(lines)!=1:raise RuntimeError('No unique '+prefix+' in remote output')
    return base64.b64decode(lines[0],validate=True)


def remote_probe(infra,site,files):
    """Check provider-root free disk, existing file sizes; don't create files."""
    # Use the original guest's Python venv (same as Phase 1).
    py=infra.Q05_CLOUD_VENV[site]
    command=(f'test -d /opt/egressopt; test -x {shlex.quote(py)}; '
      f'{shlex.quote(py)} -c '+shlex.quote('import duckdb;assert duckdb.__version__=="1.5.6"')+'; '
      'echo M6L_FREE_BYTES=$(df -PB1 /opt/egressopt | tail -1 | awk \'{print $4}\'); '
      'echo M6L_SF1_UNTOUCHED=$(test -d /opt/egressopt/data/tpch_sf1 && echo yes || echo unknown)')
    result=remote_script(infra,site,command,'storage-probe')
    m=re.findall(r'M6L_FREE_BYTES=(\d+)',result)
    if len(m)!=1:raise RuntimeError('Cannot determine available VM filesystem bytes for '+site)
    free=int(m[0]);needed=sum(v[0] for v in files.values())
    # Conservative 2 GiB headroom, even if volume is advertised as 100GB.
    if free<needed+2*1024**3:
        raise RuntimeError(f'{site}: insufficient verified disk space ({free} B available; '
                           f'{needed} B files plus 2 GiB reserve)')
    return {'free_bytes':free,'required_bytes':needed,'vm_python':py}


def remote_install(infra,site,work,token,ip):
    """Install temporary service using a single compressed base64 shell upload."""
    import shlex as sq
    manifest={'site':site,'files':{t:{'bytes':v[0],'rows':v[1],'sha256':v[2]}
                                   for t,v in FILE_LIST[site].items()}}
    payload=base64.b64encode(gzip.compress(REMOTE_WORKER_SOURCE.encode(),compresslevel=9)).decode()
    config=base64.b64encode(json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()).decode()
    workq=sq.quote(work)
    command=(f'umask 077; mkdir -p {workq}; '
      f'printf %s {sq.quote(payload)} | base64 -d | gzip -d > {workq}/receiver.py; '
      f'printf %s {sq.quote(config)} | base64 -d > {workq}/allowlist.json; '
      f'openssl req -x509 -nodes -newkey rsa:2048 -sha256 -days 1 '
      f'-keyout {workq}/server.key -out {workq}/server.pem '
      f'-subj /CN=egressopt-m6l -addext basicConstraints=critical,CA:TRUE '
      f'-addext subjectAltName=IP:{ip} >/dev/null 2>&1; '
      f'echo M6L_CERT_B64=$(base64 -w0 {workq}/server.pem)')
    out=remote_script(infra,site,command,'install-receiver')
    cert=b64_marker(out,'M6L_CERT_B64=')
    if b'BEGIN CERTIFICATE' not in cert:raise RuntimeError('Invalid TLS leaf PEM')
    launch='\n'.join(['#!/bin/sh','set -eu',
        'export M6L_TOKEN='+sq.quote(token),
        'exec '+sq.quote(infra.Q05_CLOUD_VENV[site])+' '+sq.quote(work+'/receiver.py')+
        ' --cert '+sq.quote(work+'/server.pem')+' --key '+sq.quote(work+'/server.key')+
        ' --manifest '+sq.quote(work+'/allowlist.json')])+'\n'
    coded=base64.b64encode(launch.encode()).decode()
    unit='egressopt-m6l-sf10-'+site
    command=(f'printf %s {sq.quote(coded)} | base64 -d > {workq}/launch.sh; '
          f'chmod 700 {workq}/launch.sh; '
          f'systemd-run --unit={sq.quote(unit)} --collect --property=Restart=no '
          f'/bin/sh {workq}/launch.sh >/dev/null; '
          'for i in $(seq 1 30); do '
          f'CODE=$(curl -k -s -o /dev/null -w "%{{http_code}}" --max-time 3 '
          'https://127.0.0.1:18443/health || true); '
          '[ "$CODE" = 401 ] && break; sleep 1; done; '
          'test "$CODE" = 401; echo M6L_RECEIVER_READY')
    if 'M6L_RECEIVER_READY' not in remote_script(infra,site,command,'launch-receiver'):
        raise RuntimeError('M6L receiver launch failed: '+site)
    return cert,unit


def allow_ingress(infra,site,source_ip,run_id,state):
    """Allow Mac's /32 to temporary HTTPS port, record exact created rule IDs."""
    if site=='aws':
        aws=infra.q05_aws_instance();groups=aws['SecurityGroups']
        if len(groups)!=1:raise RuntimeError('AWS VM expected to have exactly one security group')
        group=groups[0]['GroupId'];state['aws_group']=group
        permission=[{'IpProtocol':'tcp','FromPort':PORT,'ToPort':PORT,
          'IpRanges':[{'CidrIp':source_ip+'/32','Description':'EgressOpt M6L SF10 temporary upload'}]}]
        created=infra.q05_aws('ec2','authorize-security-group-ingress',
                              '--group-id',group,'--ip-permissions',json.dumps(permission))
        rules=created.get('SecurityGroupRules',[])
        if len(rules)!=1 or not rules[0].get('SecurityGroupRuleId'):
            raise RuntimeError('AWS ingress creation returned no unique rule ID')
        state['aws_rule']=rules[0]['SecurityGroupRuleId']
    elif site=='azure':
        prior=json.loads(run_cmd(infra,['az','network','nsg','rule','list','--resource-group',
                         infra.AZ_RG,'--nsg-name',infra.AZ_NSG,'--output','json']))
        used={int(x['priority']) for x in prior if x.get('direction')=='Inbound'}
        priority=next((x for x in range(1100,2500) if x not in used),None)
        if priority is None:raise RuntimeError('Azure NSG has no free ingress priority')
        name='egressopt-m6l-'+run_id
        run_cmd(infra,['az','network','nsg','rule','create','--resource-group',infra.AZ_RG,
                     '--nsg-name',infra.AZ_NSG,'--name',name,'--priority',str(priority),
                     '--direction','Inbound','--access','Allow','--protocol','Tcp',
                     '--source-address-prefixes',source_ip+'/32',
                     '--destination-port-ranges',str(PORT),'--output','none'])
        state['azure_rule']=name
    elif site=='gcp':
        gcp=infra.q05_gcp_instance()
        network=gcp['networkInterfaces'][0]['network'].rsplit('/',1)[-1]
        tag='egressopt-m6l-'+run_id
        infra.q05_gcloud('compute','instances','add-tags',infra.GCP_VM,
                         '--zone='+infra.GCP_ZONE,'--tags='+tag,'--quiet','--format=json')
        state['gcp_tag']=tag
        infra.q05_gcloud('compute','firewall-rules','create',tag,
                         '--network='+network,'--direction=INGRESS',
                         '--action=ALLOW','--rules=tcp:'+str(PORT),
                         '--source-ranges='+source_ip+'/32',
                         '--target-tags='+tag,'--quiet','--format=json')
        state['gcp_rule']=tag
    else:raise ValueError(site)


def tls_request(ip,cert,token,method,route,path=None,meta=None):
    context=ssl.create_default_context(cadata=cert.decode('ascii'))
    context.check_hostname=True
    context.minimum_version=ssl.TLSVersion.TLSv1_2
    conn=http.client.HTTPSConnection(ip,PORT,timeout=240,context=context)
    try:
        conn.putrequest(method,route)
        conn.putheader('Authorization','Bearer '+token)
        if path is not None:
            conn.putheader('Content-Length',str(path.stat().st_size))
            conn.putheader('X-Expected-SHA256',meta[2])
        conn.endheaders()
        if path is not None:
            with path.open('rb') as f:
                sent=0
                for block in iter(lambda:f.read(4*1024*1024),b''):
                    conn.send(block);sent+=len(block)
            if sent!=meta[0]:raise RuntimeError('Local input changed during streaming')
        response=conn.getresponse()
        body=response.read(10000)
        try:payload=json.loads(body)
        except Exception:raise RuntimeError('Unexpected HTTPS receiver reply')
        if response.status!=200:raise RuntimeError(f'HTTPS {response.status}: {payload}')
        return payload
    finally:conn.close()


def transfer_site(site,ip,cert,token,report):
    for name,meta in FILE_LIST[site].items():
        status=tls_request(ip,cert,token,'GET','/status/'+name)
        if status.get('state')=='mismatch':
            raise RuntimeError(f'{site}/{name} has preexisting mismatched data; refusing overwrite')
        if status.get('state')=='verified':result=status;action='existing_verified'
        else:
            print(f'  Upload {site}/{name}: {meta[0]/1e6:.2f} MB',flush=True)
            result=tls_request(ip,cert,token,'PUT','/data/'+name,LOCAL_ROOT/(name+'.parquet'),meta)
            action='uploaded'
        if (result.get('state'),result.get('bytes'),result.get('rows'),result.get('sha256'))!=\
                ('verified',meta[0],meta[1],meta[2]):
            raise RuntimeError(f'{site}/{name}: remote checksum / rows / size verification failed')
        report['verified_files'].append({'site':site,'table':name,'bytes':meta[0],
                'rows':meta[1],'sha256':meta[2],'action':action})


def status_write(report,where):
    where.parent.mkdir(parents=True,exist_ok=True)
    where.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')


def apply(ack=False,source_ip=None):
    if not ack:raise RuntimeError('VM startup incurs charges: --ack-cloud-cost is required')
    local=preflight()
    infra=load_phase1()
    ip_source=my_public_ip(source_ip)
    suffix=secrets.token_hex(3)
    output=ROOT/'results'/'phase2'/'m6l'/('sf10_deployment_'+suffix+'.json')
    # Never write access tokens, IPs or TLS certificates to shareable reports.
    report={'status':'IN_PROGRESS','protocol':'M6L_DIRECT_PINNED_TLS_UPLOAD','sf':10,
            'source_manifest_verified_locally':True,'bytes_planned':local['total_bytes'],
            'sources':list(FILE_LIST),'verified_files':[],'cleanup':{},
            'temporary_rules_removed':False,'virtual_machines_restored':False,
            'cloud_execution_attempted':True,'query_execution_attempted':False,
            'phase1_code_changed':False,'s3_used':False,'destination_root':DEST_ROOT}
    state={'started':{},'services':{},'rules':{}}
    status_write(report,output)
    token=secrets.token_urlsafe(48)
    work='/opt/egressopt/runtime/m6l_sf10_'+suffix
    ips={};certs={}
    try:
        for site in FILE_LIST:
            old={'aws':lambda:infra.q05_aws_instance()['State']['Name'],
                 'azure':infra.q05_azure_state,
                 'gcp':lambda:infra.q05_gcp_instance()['status']}[site]()
            allowed=('stopped','running','PowerState/deallocated','PowerState/stopped',
                     'PowerState/running','TERMINATED','RUNNING','STOPPED')
            if old not in allowed:raise RuntimeError('Unrecognized '+site+' VM state: '+str(old))
            state['started'][site]=old in ('stopped','PowerState/deallocated','PowerState/stopped',
                                           'TERMINATED','STOPPED')
            print('Starting/checking existing '+site+' VM...',flush=True)
            infra.q05_start_vm(site)
            remote_probe(infra,site,FILE_LIST[site])
            if site=='aws':
                ips[site]=public_ipv4(infra.q05_aws_instance()['PublicIpAddress'])
            elif site=='azure':
                entry=json.loads(run_cmd(infra,['az','network','public-ip','show','--resource-group',
                           infra.AZ_RG,'--name',infra.AZ_PUBLIC,'--output','json']))
                ips[site]=public_ipv4(entry['ipAddress'])
            else:
                instance=infra.q05_gcp_instance()
                if not any(x.get('natIP') for nic in instance['networkInterfaces']
                           for x in nic.get('accessConfigs',[])):
                    state['gcp_nat_added']=True
                    infra.q05_gcloud('compute','instances','add-access-config',infra.GCP_VM,
                        '--zone='+infra.GCP_ZONE,'--network-interface=nic0',
                        '--access-config-name=external-nat','--quiet','--format=json')
                    instance=infra.q05_gcp_instance()
                ips[site]=infra.q05_gcp_public(instance)
            # Register cleanup ownership before a partially successful launch.
            state['services'][site]='egressopt-m6l-sf10-'+site
            cert,unit=remote_install(infra,site,work,token,ips[site])
            certs[site]=cert;state['services'][site]=unit
            allow_ingress(infra,site,ip_source,suffix,state)
            # Cloud firewall propagation is eventually consistent; retry only GET.
            health=None
            for attempt in range(12):
                try:
                    health=tls_request(ips[site],cert,token,'GET','/health')
                    break
                except (OSError,TimeoutError) as err:
                    if attempt==11:raise RuntimeError(
                        'Temporary HTTPS ingress could not be reached for '+site+
                        ' after firewall propagation; verify Mac public IP and guest firewall') from err
                    time.sleep(3)
            if health.get('status')!='ok' or health.get('site')!=site or health.get('duckdb')!='1.5.6':
                raise RuntimeError('Unexpected authenticated TLS receiver: '+site)
            transfer_site(site,ips[site],cert,token,report)
            status_write(report,output)
        report['status']='SF10_CLOUD_FILES_VERIFIED'
    except BaseException as error:
        report['status']='FAILED';report['failure']=type(error).__name__+': '+str(error)[:500]
        raise
    finally:
        print('M6L cleanup: services, ingress rules, and VMs started by this run...',flush=True)
        for site,unit in state['services'].items():
            try:
                remote_script(infra,site,
                   'systemctl stop '+shlex.quote(unit)+' >/dev/null 2>&1 || true; '+
                   'rm -rf '+shlex.quote(work)+'; echo M6L_WORKER_REMOVED','cleanup-receiver')
                report['cleanup']['service_'+site]='removed'
            except Exception as e:report['cleanup']['service_'+site]='FAILED '+str(e)[:220]
        if state.get('aws_rule'):
            try:
                infra.q05_aws('ec2','revoke-security-group-ingress',
                  '--group-id',state['aws_group'],'--security-group-rule-ids',state['aws_rule'])
                report['cleanup']['aws_rule']='removed'
            except Exception as e:report['cleanup']['aws_rule']='FAILED '+str(e)[:220]
        if state.get('azure_rule'):
            try:
                run_cmd(infra,['az','network','nsg','rule','delete','--resource-group',infra.AZ_RG,
                    '--nsg-name',infra.AZ_NSG,'--name',state['azure_rule'],'--output','none'])
                report['cleanup']['azure_rule']='removed'
            except Exception as e:report['cleanup']['azure_rule']='FAILED '+str(e)[:220]
        if state.get('gcp_rule'):
            try:
                infra.q05_gcloud('compute','firewall-rules','delete',state['gcp_rule'],'--quiet')
                report['cleanup']['gcp_rule']='removed'
            except Exception as e:report['cleanup']['gcp_rule']='FAILED '+str(e)[:220]
        if state.get('gcp_tag'):
            try:
                infra.q05_gcloud('compute','instances','remove-tags',infra.GCP_VM,
                    '--zone='+infra.GCP_ZONE,'--tags='+state['gcp_tag'],'--quiet','--format=json')
                report['cleanup']['gcp_tag']='removed'
            except Exception as e:report['cleanup']['gcp_tag']='FAILED '+str(e)[:220]
        if state.get('gcp_nat_added'):
            try:
                infra.q05_gcloud('compute','instances','delete-access-config',infra.GCP_VM,
                     '--zone='+infra.GCP_ZONE,'--network-interface=nic0',
                     '--access-config-name=external-nat','--quiet','--format=json')
                report['cleanup']['gcp_nat']='removed'
            except Exception as e:report['cleanup']['gcp_nat']='FAILED '+str(e)[:220]
        for site,started in state['started'].items():
            if started:
                try:
                    infra.q05_remote(site,'sync; echo M6L_SYNC_COMPLETE','sync-persistent-data')
                    if site=='aws':
                        infra.q05_aws('ec2','stop-instances','--instance-ids',infra.AWS_INSTANCE)
                        run_cmd(infra,['aws','ec2','wait','instance-stopped','--instance-ids',
                                       infra.AWS_INSTANCE,'--profile',infra.AWS_PROFILE,
                                       '--region',infra.AWS_REGION])
                    elif site=='azure':
                        run_cmd(infra,['az','vm','deallocate','--resource-group',infra.AZ_RG,
                                       '--name',infra.AZ_VM,'--output','none'])
                    else:
                        infra.q05_gcloud('compute','instances','stop',infra.GCP_VM,
                                     '--zone='+infra.GCP_ZONE,'--quiet','--format=json')
                    report['cleanup']['vm_'+site]='stopped'
                except Exception as e:report['cleanup']['vm_'+site]='FAILED '+str(e)[:220]
            else:report['cleanup']['vm_'+site]='left_running_preexisting'
        report['temporary_rules_removed']=all(not str(v).startswith('FAILED') for k,v in report['cleanup'].items() if 'rule' in k or 'tag' in k or 'nat' in k)
        report['virtual_machines_restored']=all(not str(v).startswith('FAILED') for k,v in report['cleanup'].items() if k.startswith('vm_'))
        report['cleanup_complete']=all(not str(v).startswith('FAILED') for v in report['cleanup'].values())
        if report['status']=='SF10_CLOUD_FILES_VERIFIED' and not report['cleanup_complete']:
            report['status']='SF10_FILES_VERIFIED_CLEANUP_INCOMPLETE'
        status_write(report,output)
        print('Report:',output,'|',report['status'],'| cleanup_complete=',report['cleanup_complete'])
    return report


def main():
    parser=argparse.ArgumentParser(description='M6L — SF10 direct VM storage placement')
    parser.add_argument('--mode',choices=('preflight','apply'),required=True)
    parser.add_argument('--ack-cloud-cost',action='store_true')
    parser.add_argument('--source-ip',help='Mac public IPv4 for temporary firewall /32 (optional)')
    args=parser.parse_args()
    if args.mode=='preflight':
        report=preflight()
        destination=ROOT/'results'/'phase2'/'m6l'/'sf10_local_preflight.json'
        status_write(report,destination)
        print('M6L_SF10_LOCAL_PREFLIGHT_PASS',report['total_bytes'],'bytes')
        print('Evidence:',destination)
    else:
        outcome=apply(args.ack_cloud_cost,args.source_ip)
        if outcome['status']!='SF10_CLOUD_FILES_VERIFIED' or not outcome['cleanup_complete']:
            raise RuntimeError('Deployment or cloud cleanup incomplete; inspect report, do not start SF10 experiments')
        print('M6L_SF10_PROVIDER_DATA_VERIFIED')

if __name__=='__main__':
    try:main()
    except Exception as e:
        print('M6L_SF10_FAIL:',type(e).__name__+':',e,file=sys.stderr)
        sys.exit(1)
