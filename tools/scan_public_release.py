#!/usr/bin/env python3
"""Local static check. Does not upload files or contact a network.

A pass reduces accidental exposure but is not a full secrets audit.
"""
import pathlib, re, sys
root = pathlib.Path(sys.argv[1] if len(sys.argv)>1 else '.').resolve()
exclude={'.git','.venv','venv','__pycache__','.pytest_cache','results','data'}
patterns={
 'AWS access key ID': re.compile(r'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b'),
 'Private-key PEM header': re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),
 'GitHub personal token': re.compile(r'gh[pousr]_[A-Za-z0-9_]{20,}'),
 'Bearer token literal': re.compile(r'Bearer\s+[A-Za-z0-9._~-]{32,}',re.I),
 'Nonlocal IPv4 literal': re.compile(r'(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])'),
 'AWS instance ID literal': re.compile(r'\bi-[0-9a-f]{8,17}\b',re.I),
 'Home directory path': re.compile(r'/Users/[^/\s]+/'),
}
findings=[];checked=0
for p in root.rglob('*'):
 if not p.is_file() or p.resolve()==pathlib.Path(__file__).resolve() or any(part in exclude for part in p.relative_to(root).parts):continue
 if p.name in ('.env','cloud.env') or (p.name.startswith('.env.') and p.name!='.env.example'):
  findings.append((p.relative_to(root),'private configuration filename'));continue
 if p.suffix.lower() not in {'.py','.tex','.sql','.json','.md','.txt','.csv','.example','.toml','.yaml','.yml','.sh'}:continue
 data=p.read_text(encoding='utf-8',errors='replace');checked+=1
 for name,pat in patterns.items():
  for m in pat.finditer(data):
   if name=='Nonlocal IPv4 literal' and (m.group(0) in ('127.0.0.1','0.0.0.0','255.255.255.255','35.235.240.0','8.8.8.8','8.8.4.4','1.1.1.1','9.9.9.9') or m.group(0).startswith(('192.0.2.','198.51.100.','203.0.113.'))):continue
   findings.append((p.relative_to(root),name));break
# permit documentation with patterns, but flag as manual review
print(f'Scanned {checked} text files; potential findings: {len(findings)}')
for f,kind in findings:print('REVIEW',f,kind)
sys.exit(1 if findings else 0)
