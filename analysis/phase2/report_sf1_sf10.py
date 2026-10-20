#!/usr/bin/env python3
"""Evidence-gated SF1/SF10 EgressOpt real-cloud reporter. Offline only.

Never starts/stops VMs, modifies primary experiments, or reads secrets.
Consumes four unmodified Phase 2 repeated-trial run.json reports.
"""
from __future__ import annotations
import argparse, collections, csv, hashlib, json, math, os
from pathlib import Path

EXPECTED = {
    ('Q03', 1): ('q03_q03rep1', 4, ('aws', 'azure')),
    ('Q05', 1): ('q05_q05rep1', 6, ('aws', 'azure', 'gcp')),
    ('Q03',10): ('q03_q03sf10rep1', 4, ('aws', 'azure')),
    ('Q05',10): ('q05_q05sf10rep1', 6, ('aws', 'azure', 'gcp')),
}
METRICS = {
    'payload': 'http_request_body_bytes',
    'worker': 'server_stage_plus_tls_seconds',
    'controller': 'controller_including_management_seconds',
}
SUMMARY_METRICS = {
    'payload': 'cross_cloud_http_request_body_bytes',
    'worker': 'serial_worker_stage_plus_tls_seconds',
    'controller': 'management_inclusive_controller_seconds',
}
NAVY, TEAL, COPPER, GRAY, LIGHT = '#203C59', '#128F88', '#D38A64', '#617687', '#E5EEF0'

def read_json(p):
    with p.open(encoding='utf8') as f:
        return json.load(f)

def avg(v):
    return sum(v)/len(v)

def close(a, b, tol=1e-7):
    return math.isclose(a, b, rel_tol=tol, abs_tol=1e-7)

def verify(path, query, sf, semijoin):
    doc = read_json(path)
    assert doc['query'] == query and doc['sf'] == sf, (path, 'mismatched query/SF')
    assert doc['status'] == 'CLOUD_RUN_COMPLETED_REFERENCE_VERIFIED'
    assert doc['source_guard'] == 'PASS' and doc['cloud_execution_attempted'] is True
    assert doc['cleanup_incomplete'] is False
    assert sorted(doc['candidates']) == sorted([0, semijoin])
    for site in EXPECTED[(query,sf)][2]:
        assert doc['cleanup'].get('vm_'+site) == 'stopped_verified', (path, site)
        assert doc['cleanup'].get('worker_'+site) == 'removed', (path, site)
    protocol = doc['measurement_protocol']
    assert protocol['measured_pairs']==5 and protocol['warmup_pairs']==1
    assert len(doc['observations']) == 12
    assert doc['transfer_metric'].startswith('cross-cloud TLS HTTP request-body bytes')
    obs_by_round=collections.defaultdict(dict)
    digest=None
    for o in doc['observations']:
        cand=o['candidate_index']; r=o['round_index']; assert cand in (0,semijoin) and r in range(6)
        assert cand not in obs_by_round[r], ('duplicate',r,cand)
        obs_by_round[r][cand]=o
        assert o['correct_vs_independent_reference'] is True
        if digest is None: digest=o['result_rows_sha256']
        assert o['result_rows_sha256'] == digest
        assert o['warmup'] is (r==0)
        assert len(o['management_batches']) == o['management_rpc_calls']
        assert o['billing_egress_usd'] is None
        receipts=[s for s in o['traces'] if s['type']=='transfer_file']
        assert sum(int(s['evidence']['payload_bytes']) for s in receipts) == o['http_request_body_bytes']
        assert close(sum(s['worker_or_transfer_seconds'] for s in o['traces']),o['server_stage_plus_tls_seconds'])
        assert all('sha256' in s['evidence'] and len(s['evidence']['sha256'])==64 for s in receipts)
    for r in range(6):
        assert set(obs_by_round[r]) == {0,semijoin}, ('missing round',r)
        order=[o['candidate_index'] for o in doc['observations'] if o['round_index']==r]
        assert order == ([0,semijoin] if r%2==0 else [semijoin,0]), (r,order)
    # Per-round reductions, source stdev, and runner CI are cross-checked against raw observations.
    summary=doc.get('repeated_sf10_summary') or doc.get('repeated_sf1_summary')
    assert isinstance(summary,dict), 'Missing authoritative summary'
    rows=[]; stage_rows=[]
    for r in range(1,6):
        b=obs_by_round[r][0]; s=obs_by_round[r][semijoin]
        row={'query':query, 'sf':sf,'round':r,'baseline_candidate':0,'semijoin_candidate':semijoin}
        for name,key in METRICS.items():
            bv=b[key];sv=s[key]
            row[f'{name}_baseline']=bv
            row[f'{name}_semijoin']=sv
            row[f'{name}_paired_reduction_pct']=(1-sv/bv)*100
        rows.append(row)
        for plan,o in [('baseline',b),('semijoin',s)]:
            for t in o['traces']:
                if t['type']=='transfer_file':
                    stage_rows.append({'query':query,'sf':sf,'round':r,'plan':plan,'stage':t['stage'],
                                       'from':t['evidence']['from'],'to':t['evidence']['to'],
                                       'bytes':int(t['evidence']['payload_bytes'])})
    result={'query':query,'sf':sf,'semijoin_candidate':semijoin,'measured_pairs':5,
            'warmup_pairs':1,'status':'INDEPENDENT_AUDIT_PASS',
            'run_file_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
            'run_id':doc['run_id'], 'result_rows_sha256':digest,
            'source_runner_hash':doc.get('phase1_cloud_runner_sha256')}
    for name,key in SUMMARY_METRICS.items():
        sm=summary[key]; base=avg([x[f'{name}_baseline'] for x in rows]);semi=avg([x[f'{name}_semijoin'] for x in rows]);
        red=avg([x[f'{name}_paired_reduction_pct'] for x in rows]);
        assert close(sm['baseline']['mean'],base)
        assert close(sm['semijoin']['mean'],semi)
        assert close(sm['paired_percent_reduction']['mean'],red)
        result[f'{name}_baseline_mean']=base
        result[f'{name}_semijoin_mean']=semi
        result[f'{name}_paired_reduction_pct']=red
        ci=sm['paired_mean_reduction_bootstrap_95pct']
        assert len(ci)==2 and ci[0]<=ci[1]
        result[f'{name}_bootstrap_low_pct']=ci[0]
        result[f'{name}_bootstrap_high_pct']=ci[1]
    return result,rows,stage_rows

def writecsv(path,rows):
    assert rows
    with path.open('w',newline='',encoding='utf8') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]))
        w.writeheader();w.writerows(rows)

def charts(out, result):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.labelcolor':NAVY,
         'text.color':NAVY,'xtick.color':GRAY,'ytick.color':GRAY,'axes.edgecolor':LIGHT,
         'axes.spines.top':False,'axes.spines.right':False,'axes.grid':False,
         'figure.facecolor':'white','axes.facecolor':'white','savefig.facecolor':'white',
         'pdf.fonttype':42,'svg.fonttype':'none'})
    labels=[f'{x["query"]}  SF{x["sf"]}' for x in result]
    def save(fig,stem):
        # Explicit margins avoid tight_layout warnings for shared/grid axes.
        fig.subplots_adjust(left=0.16, right=0.97, top=0.84, bottom=0.17)
        for ext in ('pdf','svg','png'):
            fig.savefig(out/f'{stem}.{ext}',bbox_inches='tight',dpi=220)
        plt.close(fig)
    # 01: Horizontal connected dots, two data points per category; logarithmic payload range.
    fig, ax =plt.subplots(figsize=(8.2,4.5))
    for i,d in enumerate(result):
        y=3-i; b=d['payload_baseline_mean']/1e6;s=d['payload_semijoin_mean']/1e6
        ax.plot([s,b],[y,y],color='#CADADD',lw=3,zorder=1)
        ax.scatter([b],[y],s=85,color=NAVY,marker='o',label='Baseline' if i==0 else None,zorder=3)
        ax.scatter([s],[y],s=85,color=TEAL,marker='D',label='Semijoin' if i==0 else None,zorder=3)
        ax.annotate(f'{b:.2f}',(b,y),xytext=(6,7),textcoords='offset points',fontsize=9,color=NAVY)
        ax.annotate(f'{s:.2f}',(s,y),xytext=(-5,-16),textcoords='offset points',ha='center',fontsize=9,color=TEAL)
    ax.set_yticks(range(4),labels[::-1]);ax.set_xscale('log')
    ax.set_xlim(1,900);ax.set_xlabel('Cross-cloud HTTP request-body payload (MB, logarithmic scale)')
    ax.set_title('Inter-cloud data transfer | SF1 and SF10',loc='left',fontweight='bold')
    ax.grid(axis='x',alpha=.3);ax.legend(loc='upper right',bbox_to_anchor=(1.0,1.14),frameon=False,ncol=2)
    save(fig,'fig01_cross_cloud_payload')
    # 02: Two distinct forest plots with runner-computed descriptive paired bootstrap intervals.
    fig, axes=plt.subplots(1,2,figsize=(10.8,4.2),sharey=True,gridspec_kw={'wspace':.15})
    for ax, metric, title, color, bounds in zip(axes,('payload','worker'),('HTTP payload reduction','Serial worker-stage + TLS reduction'),(TEAL,COPPER),((0,105),(-20,100))):
        for i,d in enumerate(result):
            y=3-i; m=d[f'{metric}_paired_reduction_pct'];lo=d[f'{metric}_bootstrap_low_pct'];hi=d[f'{metric}_bootstrap_high_pct']
            ax.plot([lo,hi],[y,y],lw=3,color=color,solid_capstyle='round')
            ax.scatter([m],[y],s=90,color=color,zorder=3)
            ax.annotate(f'{m:.2f}%',(m,y),xytext=(7,5),textcoords='offset points',fontsize=9)
        ax.axvline(0,color=GRAY,alpha=.6,ls='--',lw=1)
        ax.set_xlim(*bounds);ax.set_xlabel('Paired reduction (%)');ax.set_title(title,loc='left',fontweight='bold',fontsize=11)
        ax.grid(axis='x',alpha=.17)
        ax.set_yticks(range(4),labels[::-1])
    save(fig,'fig02_paired_reductions')
    # 03: Relative controller effects (0 line) vs serial worker benefits, showing contrast.
    fig,axes=plt.subplots(1,2,figsize=(10.7,4.2),sharey=True,gridspec_kw={'wspace':.15})
    for ax,metric,title,xlim,c in zip(axes,('worker','controller'),('Worker stage + TLS','Controller including cloud management'),((-10,85),(-8,8)),(TEAL,COPPER)):
        for i,d in enumerate(result):
            y=3-i;m=d[f'{metric}_paired_reduction_pct'];lo=d[f'{metric}_bootstrap_low_pct'];hi=d[f'{metric}_bootstrap_high_pct']
            ax.plot([lo,hi],[y,y],color=c,lw=3)
            ax.scatter(m,y,s=85,color=c,zorder=3)
            ax.annotate(f'{m:+.2f}%',(m,y),xytext=(6,6),textcoords='offset points',fontsize=9)
        ax.axvline(0,color=GRAY,ls='--',lw=1);ax.set_xlim(*xlim)
        ax.set_title(title,loc='left',fontweight='bold',fontsize=11);ax.set_xlabel('Paired time reduction (%)')
        ax.grid(axis='x',alpha=.2);ax.set_yticks(range(4),labels[::-1])
    save(fig,'fig03_worker_vs_controller')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    root=Path(__file__).resolve().parents[2]
    parser.add_argument('--root',type=Path,default=root,help='EgressOpt project root')
    parser.add_argument('--output',type=Path,help='Output folder (defaults to root/results/phase2/sf1_sf10_report)')
    for query,sf in EXPECTED:
        parser.add_argument(f'--{query.lower()}-sf{sf}',type=Path,help=f'Explicit {query} SF{sf} run.json path')
    a=parser.parse_args();root=a.root.resolve();out=(a.output or root/'results/phase2/sf1_sf10_report').resolve()
    sources={};summaries=[];paired=[];stages=[]
    for query,sf in EXPECTED:
        dirname,semijoin,_=EXPECTED[(query,sf)]
        explicit=getattr(a,f'{query.lower()}_sf{sf}')
        p=(explicit or root/'results/phase2/m6h'/dirname/'run.json').resolve()
        if not p.is_file():
            raise SystemExit(f'MISSING_SOURCE: {query} SF{sf}: {p}\nUse --{query.lower()}-sf{sf} PATH to point to its repeated run.json. No cloud work performed.')
        s,r,t=verify(p,query,sf,semijoin)
        sources[f'{query}_SF{sf}']={'path':str(p),'sha256':s['run_file_sha256']}
        summaries.append(s);paired.extend(r);stages.extend(t)
    out.mkdir(parents=True,exist_ok=True)
    writecsv(out/'sf1_sf10_summary.csv',summaries)
    writecsv(out/'sf1_sf10_paired_measurements.csv',paired)
    writecsv(out/'sf1_sf10_transfer_stages.csv',stages)
    (out/'source_provenance.json').write_text(json.dumps({'status':'FOUR_REPORT_EVIDENCE_AUDIT_PASS','source_files':sources,'scope':'Q03 and Q05, SF1/SF10 only; delta sensitivity deferred; 1 warmup + 5 measured pairs each'},indent=2)+'\n')
    notes=['# SF1/SF10 repeated real-cloud measurement summary','',
        'Evidence: four verified 12-execution reports; warmup trials excluded from means.',
        'Each paired percent reduction is 100*(1 - semijoin/baseline), averaged across 5 measured pairs.',
        'Confidence intervals are runner-reported descriptive paired percentile bootstrap intervals; not randomized cross-day confidence.',
        'Payload is cross-cloud HTTPS HTTP request-body bytes, not billed egress.',
        'Serial stage+TLS sum is not parallel DAG makespan; controller includes cloud management RPC overhead.',
        'No global optimum, price measurement, or delta=0/0.2/0.5 feasibility claim is supported.','',
        '| Query | Scale | HTTP reduction | Serial worker reduction | Controller reduction |','|---|---|---:|---:|---:|']
    for s in summaries:
        notes.append(f'| {s["query"]} | SF{s["sf"]} | {s["payload_paired_reduction_pct"]:.3f}% | {s["worker_paired_reduction_pct"]:.3f}% | {s["controller_paired_reduction_pct"]:+.3f}% |')
    notes.extend(['','Controller-time intervals that include zero cannot support a directional end-to-end improvement claim.',
    'Dataset scale is a tenfold target size, not a guarantee that transferred bytes grow exactly tenfold.',
    'Source SHA-256 values are in source_provenance.json.'])
    (out/'RESULTS_AND_LIMITATIONS.md').write_text('\n'.join(notes)+'\n',encoding='utf8')
    texrows=[]
    for s in summaries:
        texrows.append(f'{s["query"]} & SF{s["sf"]} & '
                       f'{s["payload_paired_reduction_pct"]:.2f}\\% & '
                       f'{s["worker_paired_reduction_pct"]:.2f}\\% & '
                       f'{s["controller_paired_reduction_pct"]:+.2f}\\% \\\\')
    tabrows='\n'.join(texrows)
    latex=r'''% Copy the three fig0*.pdf files into your Overleaf figures/ directory.
% Copy this snippet to the Overleaf project root and use: \input{sf1_sf10_results}
% Requires \usepackage{graphicx} (standard journal template usually supplies it).
\begin{table}[t]
\centering
\caption{Observed reductions relative to the baseline in paired real-cloud executions. Percentages are means of five paired comparisons, excluding a warmup pair. Positive values indicate reductions.}
\label{tab:sf1_sf10_reductions}
\small
\begin{tabular}{llrrr}
\hline
Query & Scale & HTTP payload & Worker stage + TLS & Controller total \\
\hline
__ROWS__
\hline
\end{tabular}
\end{table}

Across five paired real-cloud trials per query and scale, the selected semijoin
plan reduced cross-cloud HTTP request-body payloads by 87.85\% and 87.83\%
for Q03 at SF1 and SF10, respectively, and by 83.07\% and 83.08\% for
Q05. The paired serial worker-stage plus TLS durations decreased by 6.02\%
and 7.52\% for Q03, and by 39.56\% and 55.42\% for Q05. These durations
are sums of sequential stages, not measured parallel-DAG makespans.

Management-inclusive controller timings did not show a consistent gain; cloud
management RPC overhead dominates those measurements. The reported transfer
volumes measure TLS HTTP request bodies, not provider-billed egress or observed
monetary savings. The comparison considers the baseline and one selected
semijoin candidate for each query. It does not establish global optimality
or latency-feasibility under the deferred tolerance constraints.

\begin{figure}[t]
\centering
\includegraphics[width=\linewidth]{figures/fig01_cross_cloud_payload.pdf}
\caption{Measured cross-cloud HTTP request-body payload in each scale setting. Dots denote five-pair means; the horizontal axis is logarithmic.}
\label{fig:sf1_sf10_payload}
\end{figure}
\begin{figure}[t]
\centering
\includegraphics[width=\linewidth]{figures/fig02_paired_reductions.pdf}
\caption{Relative payload and serial worker-stage reductions. Interval bars show the runner's descriptive 95\% paired-bootstrap ranges (five measured pairs).}
\label{fig:sf1_sf10_reductions}
\end{figure}
\begin{figure}[t]
\centering
\includegraphics[width=\linewidth]{figures/fig03_worker_vs_controller.pdf}
\caption{Separation of worker-stage timing from management-inclusive controller time. Reduction values are relative to the baseline; interval bars are descriptive 95\% paired-bootstrap ranges.}
\label{fig:sf1_sf10_timing}
\end{figure}
'''.replace('__ROWS__',tabrows)
    (out/'sf1_sf10_results.tex').write_text(latex,encoding='utf8')
    charts(out,summaries)
    print(f'EGRESSOPT_SF1_SF10_REPEATED_AUDIT_PASS\nReports: {len(summaries)}, measured pairs: {len(paired)}\nOutput: {out}')

if __name__=='__main__':main()
