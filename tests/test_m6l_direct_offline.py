# Public recursive-DNS IPv4 addresses below are used only as dummy mock fixtures; no user IPs.
import base64, hashlib, importlib.util, json, os, pathlib, shutil, socket, ssl, subprocess, sys, tempfile, time
from unittest.mock import patch

module_path=pathlib.Path(__file__).resolve().parents[1]/'src'/'provision_tpch_sf10.py'
spec=importlib.util.spec_from_file_location('sf10',module_path)
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
assert p.PORT==18443
assert p.FILE_LIST['azure']['lineitem'][1]==59986052
assert sum(size for x in p.FILE_LIST.values() for size, *_ in x.values())==2034961994
assert 'os.link(tmp,dest)' in p.REMOTE_WORKER_SOURCE
# Exercise manifest size/hash preflight with synthetic small files and tamper rejection.
with tempfile.TemporaryDirectory(prefix='m6l_test_') as d:
    root=pathlib.Path(d); data=root/'data/tpch/parquet_sf10';data.mkdir(parents=True)
    manifest_path=root/'results/dataset_validation/parquet_sf10_manifest.json';manifest_path.parent.mkdir(parents=True)
    blob=b'TPC-H dummy rows for a mock receiver\n'*10000
    digest=hashlib.sha256(blob).hexdigest()
    (data/'orders.parquet').write_bytes(blob)
    manifest_path.write_text(json.dumps({'scale_factor':10,'tables':{'orders':{'bytes':len(blob),'rows':1,'sha256':digest}}}))
    with patch.object(p,'ROOT',root),patch.object(p,'LOCAL_ROOT',data),patch.object(p,'FILE_LIST',{'aws':{'orders':(len(blob),1,digest)}}):
        assert p.preflight()['status']=='PASS'
        (data/'orders.parquet').write_bytes(blob[:-1]+b'X')
        try:p.preflight()
        except RuntimeError as e: assert 'SHA256 mismatch' in str(e)
        else:raise AssertionError('tampered file accepted')
        (data/'orders.parquet').write_bytes(blob)
        assert p.preflight()['total_bytes']==len(blob)
        # Local HTTPS streaming test with fake DuckDB count only, preserving hash/size checks.
        fake=root/'mock_python';fake.mkdir()
        (fake/'duckdb.py').write_text('''__version__="1.5.6"
class Conn:
 def __enter__(self):return self
 def __exit__(self,*a):pass
 def execute(self,*a):return self
 def fetchone(self):return (1,)
def connect(*a):return Conn()
''')
        server_src=p.REMOTE_WORKER_SOURCE.replace("DATA=Path('/opt/egressopt/data/tpch_sf10')",'DATA=Path('+repr(str(root/'server_data'))+')')
        # Test-only safety: keep the synthetic HTTPS receiver on loopback, never 0.0.0.0.
        listener="ThreadingHTTPServer(('0.0.0.0',PORT),Handler)"
        assert server_src.count(listener)==1
        server_src=server_src.replace(listener,"ThreadingHTTPServer(('127.0.0.1',PORT),Handler)")
        script=root/'receiver.py';script.write_text(server_src)
        config=root/'config.json';config.write_text(json.dumps({'site':'aws','files':{'orders':{'bytes':len(blob),'rows':1,'sha256':digest}}}))
        cert=root/'server.pem';key=root/'server.key'
        subprocess.run(['openssl','req','-x509','-nodes','-newkey','rsa:2048','-sha256','-days','1',
        '-keyout',str(key),'-out',str(cert),'-subj','/CN=egressopt-m6l',
        '-addext','basicConstraints=critical,CA:TRUE',
        '-addext','subjectAltName=IP:127.0.0.1'],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,check=True)
        with patch.dict(os.environ,{'M6L_TOKEN':'a-test-only-token','PYTHONPATH':str(fake)}):
            process=subprocess.Popen([sys.executable,str(script),'--cert',str(cert),'--key',str(key),
                     '--manifest',str(config)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                time.sleep(1.2)
                if process.poll() is not None:
                    raise AssertionError(process.stderr.read().decode())
                pem=cert.read_bytes()

                try:
                    health=p.tls_request('127.0.0.1',pem,'a-test-only-token','GET','/health')
                except Exception:
                    print('REMOTE POLL',process.poll(), 'ERR',process.stderr.read().decode() if process.poll() is not None else 'still running')
                    raise
                assert health['status']=='ok'
                assert p.tls_request('127.0.0.1',pem,'a-test-only-token','GET','/status/orders')['state']=='missing'
                answer=p.tls_request('127.0.0.1',pem,'a-test-only-token','PUT','/data/orders',data/'orders.parquet',(len(blob),1,digest))
                assert answer['sha256']==digest and answer['rows']==1
                assert (root/'server_data/orders.parquet').read_bytes()==blob
                second=p.tls_request('127.0.0.1',pem,'a-test-only-token','GET','/status/orders')
                assert second['state']=='verified'
                report={'verified_files':[]}
                with patch.object(p,'FILE_LIST',{'aws':{'orders':(len(blob),1,digest)}}):
                    p.transfer_site('aws','127.0.0.1',pem,'a-test-only-token',report)
                assert report['verified_files'][0]['action']=='existing_verified'
                try:p.tls_request('127.0.0.1',pem,'bad_token','GET','/health')
                except RuntimeError as e:assert '401' in str(e)
                else:raise AssertionError('bad token accepted')
                (root/'server_data/orders.parquet').write_bytes(blob[:-1]+b'Z')
                try:p.tls_request('127.0.0.1',pem,'a-test-only-token','PUT','/data/orders',data/'orders.parquet',(len(blob),1,digest))
                except RuntimeError as e:assert '409' in str(e)
                else:raise AssertionError('existing mismatched destination overwritten')
            finally:
                process.terminate()
                try:process.wait(timeout=3)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)
print('M6L_OFFLINE_TEST_PASS: source gates, data integrity, authenticated TLS streaming, immutability, idempotency')

# Mock full three-provider VM state transitions and fail-closed cleanup.
class FakeInfra:
    AWS_INSTANCE='i-mock'; AWS_PROFILE='research'; AWS_REGION='us-east-1'
    AZ_RG='group-mock'; AZ_PUBLIC='public-mock'; AZ_VM='azure-mock'; AZ_NSG='nsg-mock'
    GCP_VM='gcp-mock'; GCP_ZONE='us-east4-a'
    Q05_CLOUD_VENV={'aws':'/venv/python','azure':'/venv/python','gcp':'/venv/python'}
    def __init__(self):
        self.states={'aws':'stopped','azure':'PowerState/deallocated','gcp':'TERMINATED'}
        self.calls=[]
    def q05_aws_instance(self):
        return {'State':{'Name':self.states['aws']},'PublicIpAddress':'8.8.8.8',
                'SecurityGroups':[{'GroupId':'sg-mock'}]}
    def q05_azure_state(self):return self.states['azure']
    def q05_gcp_instance(self):return {'status':self.states['gcp'],
        'networkInterfaces':[{'network':'projects/m/networks/default','accessConfigs':[{'natIP':'9.9.9.9'}]}]}
    def q05_gcp_public(self,x):return '9.9.9.9'
    def q05_start_vm(self,site):
        self.calls.append(('start',site))
        self.states[site]={'aws':'running','azure':'PowerState/running','gcp':'RUNNING'}[site]
    def q05_remote(self,site,command,label):
        self.calls.append(('remote',site,label))
        return 'M6L_WORKER_REMOVED M6L_SYNC_COMPLETE'
    def q05_aws(self,*args):
        self.calls.append(('aws',args))
        if args[1]=='stop-instances':self.states['aws']='stopped'
        return {}
    def q05_gcloud(self,*args):
        self.calls.append(('gcloud',args))
        if 'stop' in args:self.states['gcp']='TERMINATED'
        return ''

with tempfile.TemporaryDirectory(prefix='m6l_vm_mock_') as d:
    root=pathlib.Path(d);infra=FakeInfra()
    def fake_cli(infra,cmd):
        if 'public-ip' in cmd:return '{"ipAddress":"8.8.4.4"}'
        if 'deallocate' in cmd:infra.states['azure']='PowerState/deallocated'
        if 'rule' in cmd and 'list' in cmd:return '[]'
        return ''
    def fake_transfer(site,ip,cert,token,report):
        for name,(size,rows,sha) in p.FILE_LIST[site].items():
            report['verified_files'].append({'site':site,'table':name,'bytes':size,'rows':rows,'sha256':sha})
    mockings=[patch.object(p,'ROOT',root),patch.object(p,'preflight',return_value={'status':'PASS','total_bytes':2034961994}),
         patch.object(p,'load_phase1',return_value=infra),patch.object(p,'my_public_ip',return_value='1.1.1.1'),
         patch.object(p,'remote_probe',return_value={'free_bytes':10**11}),
         patch.object(p,'remote_install',side_effect=lambda infra,site,work,token,ip: (b'CERT','egressopt-m6l-sf10-'+site)),
         patch.object(p,'allow_ingress',return_value=None),
         patch.object(p,'tls_request',return_value={'status':'ok','site':'unused','duckdb':'1.5.6'}),
         patch.object(p,'transfer_site',side_effect=fake_transfer),
         patch.object(p,'run_cmd',side_effect=fake_cli)]
    with mockings[0],mockings[1],mockings[2],mockings[3],mockings[4],mockings[5],mockings[6],mockings[7],mockings[8],mockings[9]:
        # health site must match, so provide a mock per source address
        def fake_tls(ip,cert,token,method,route):
            return {'status':'ok','site':{'8.8.8.8':'aws','8.8.4.4':'azure','9.9.9.9':'gcp'}[ip],
                     'duckdb':'1.5.6'}
        with patch.object(p,'tls_request',side_effect=fake_tls):
            good=p.apply(ack=True,source_ip='1.1.1.1')
    assert good['status']=='SF10_CLOUD_FILES_VERIFIED'
    assert good['cleanup_complete'] and len(good['verified_files'])==6
    assert infra.states=={'aws':'stopped','azure':'PowerState/deallocated','gcp':'TERMINATED'}
    assert all(key in good['cleanup'] for key in ('vm_aws','vm_azure','vm_gcp'))
    # No cloud started without explicit ack; preflight not called.
    try:p.apply(ack=False)
    except RuntimeError as e:assert 'ack-cloud-cost' in str(e)
    else:raise AssertionError('cost acknowledgement not enforced')
print('M6L_MOCK_VM_LIFECYCLE_PASS: 3-provider startup, checksum-report logic, cleanup, explicit cost acknowledgement')
