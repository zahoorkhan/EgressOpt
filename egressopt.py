#!/usr/bin/env python3
"""EgressOpt v0.1: conservative, dependency-free research prototype.

Commands:
  python egressopt.py selfcheck
  python egressopt.py evaluate --archive EgressOpt_Paper_Input.zip --tariffs tariffs_illustrative.json --out results_v1
  python egressopt.py select --archive ... --tariffs ... --query Q05 --slo-ms 300
  python egressopt.py memo-demo --tariffs tariffs_illustrative.json

Evidence-only evaluation is *offline held-out replay*, not prospective cloud execution.
Pricing values in the included example are synthetic scenarios, not provider invoices.
"""
from __future__ import annotations
import argparse
import collections
import csv
import hashlib
import io
import itertools
import json
import math
import pathlib
import statistics
import sys
import time
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

RUNS = {
    "Q12": "20261015T052016Z_ee6c6e83",
    "Q03": "20261015T084807Z_3a72c9b1",
    "Q14": "20261015T111506Z_fb385312",
    "Q05": "20261015T203105Z_cfdf2d72",
    "Q09": "20261016T075336Z_737221d6",
    "Q06": "20261016T123518Z_3cdc99fa",
}
ALL_SITES = ("aws", "azure", "gcp")
TRAIN_TRIALS = (1, 2, 3, 4, 5)
TEST_TRIALS = (6, 7, 8, 9, 10)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def slug(q):
    return q.lower()


def stable_hash(data):
    return hashlib.sha256(data).hexdigest()


class Archive:
    """Reads only explicitly selected successful run paths; never auto-selects a run."""
    def __init__(self, path):
        self.path = pathlib.Path(path)
        self.z = zipfile.ZipFile(self.path)
        bad = self.z.testzip()
        require(bad is None, 'Corrupt ZIP member: ' + str(bad))
        self.names = {n for n in self.z.namelist() if not n.startswith('__MACOSX/')}
        self.prefixes = {}
        for q, run in RUNS.items():
            suffix = "/" + run + "/run.json"
            found = [n[:-len('run.json')] for n in self.names if n.endswith(suffix)]
            require(len(found) == 1, f"{q}: expected unique successful run {run}, found {len(found)}")
            self.prefixes[q] = found[0]

    def read(self, q, filename):
        path = self.prefixes[q] + filename
        require(path in self.names, f"Missing {q} archive member: {filename}")
        return self.z.read(path)

    def read_json(self, q, filename):
        return json.loads(self.read(q, filename))

    def load(self):
        records = {}
        manifest = {}
        for q, id in RUNS.items():
            r = self.read_json(q, 'run.json')
            require(r['run_id'] == id, f'{q} incorrect run ID')
            require(str(r.get('execution_mode', '')).lower() == 'real_multicloud', f'{q} not real cloud')
            stat = str(r.get('status', r.get('run_status', ''))).lower()
            require(stat in ('completed', 'success'), f'{q} not completed: {stat}')
            raw = self.read(q,'trial_metrics.csv')
            if r.get('trial_metrics_sha256'):
                require(stable_hash(raw) == r['trial_metrics_sha256'], f'{q} CSV checksum mismatch')
            rows = list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))))
            require(bool(rows), f'{q} empty trial CSV')
            by_plan = collections.defaultdict(list)
            for row in rows:
                plan = row['plan_id']
                t = int(row['trial'])
                require(t in (*TRAIN_TRIALS, *TEST_TRIALS), f'{q}/{plan}: invalid trial {t}')
                flag = row.get('correct_vs_reference', row.get('correct_vs_local_sql', ''))
                require(str(flag).lower() == 'true', f'{q}/{plan}/{t}: correctness not PASS')
                elapsed = float(row['total_seconds'])
                require(math.isfinite(elapsed) and elapsed > 0, f'{q}/{plan}/{t}: bad time')
                stage = float(row['stage_seconds'])
                require(math.isfinite(stage) and stage >= 0, f'{q}/{plan}/{t}: bad stage time')
                if q in ('Q05', 'Q09'):
                    detail = self.read_json(q,f'trials/{plan}/trial_{t:03d}.json')
                    edges = detail['transfer_edges']
                elif q == 'Q12':
                    edges = [{
                        'source_cloud': row['logical_source_cloud'],
                        'destination_cloud': row['logical_destination_cloud'],
                        'curl_size_upload': int(row['intermediate_parquet_bytes']),
                        'label': 'Q12_intermediate'
                    }]
                else:
                    detail = self.read_json(q, f'trial_{t:02d}_{plan}.json')
                    edges = detail.get('transfer_edges', detail.get('transfers'))
                    require(edges is not None,f'{q}/{plan}: no edges')
                ed = []
                for e in edges:
                    s = str(e['source_cloud']).lower()
                    d = str(e['destination_cloud']).lower()
                    n = int(e['curl_size_upload'])
                    require(s in ALL_SITES and d in ALL_SITES and n >= 0, f'{q}/{plan}: bad edge')
                    require(s != d, f'{q}/{plan}: same-provider edge unexpectedly classified')
                    ed.append({'source':s,'destination':d,'bytes':n,
                               'label':str(e.get('label',e.get('name',e.get('part','intermediate'))))})
                actual_bytes = sum(e['bytes'] for e in ed)
                claimed = int(row['intermediate_parquet_bytes'])
                require(actual_bytes == claimed, f'{q}/{plan}/{t}: edge sum {actual_bytes} != {claimed}')
                by_plan[plan].append({'trial':t,'duration_s':elapsed,'stage_s':stage,
                                      'bytes':actual_bytes,'edges':ed})
            expected = {'Q12':2,'Q03':3,'Q14':4,'Q05':4,'Q09':4,'Q06':3}[q]
            require(len(by_plan) == expected,f'{q} plan count mismatch')
            for plan, vs in by_plan.items():
                require(sorted(x['trial'] for x in vs) == list(range(1,11)),f'{q}/{plan} missing trial')
            records[q] = {k:sorted(v,key=lambda x:x['trial']) for k,v in sorted(by_plan.items())}
            manifest[q] = {'run_id':id,'run_json_sha256':stable_hash(self.read(q,'run.json')),
                           'trial_metrics_sha256':stable_hash(raw),'plan_count':len(by_plan),
                           'trial_count':sum(len(x) for x in by_plan.values())}
        return records,manifest


class Tariffs:
    def __init__(self, path):
        raw=pathlib.Path(path).read_bytes(); self.sha256=stable_hash(raw)
        d=json.loads(raw)
        require(d.get('classification') in ('ILLUSTRATIVE_SCENARIO_NOT_VERIFIED_RATES','PUBLISHED_LIST_RATE_MARGINAL_SCENARIO_NOT_BILLED'),
                'Missing explicit pricing classification')
        self.data=d
        self.unit=int(d['unit_bytes'])
        require(self.unit>0,'unit_bytes must be positive')
        self.rates={}
        self.units={}
        for edge in d['directed_rates']:
            s,dst=str(edge['source']).lower(),str(edge['destination']).lower()
            rate=Decimal(str(edge['usd_per_unit']))
            require(s in ALL_SITES and dst in ALL_SITES and s!=dst and rate.is_finite() and rate>=0,'invalid tariff')
            require((s,dst) not in self.rates,'duplicate tariff edge')
            self.rates[(s,dst)]=rate
            self.units[(s,dst)]=int(edge.get('unit_bytes',self.unit))
            require(self.units[(s,dst)]>0,'invalid tariff billing unit')
        require(len(self.rates)==6,'Exactly six explicit directed pairs required')
        require(d.get('tier_assumption')=='fixed_marginal_rates_no_free_tier',
                'Only fixed marginal rates without free tiers supported in this prototype')
        self.id=d.get('scenario_id','scenario')

    def cost(self, edges):
        # Proxy monetary charge for measured HTTP bodies, not actual billed bytes or invoice charges.
        return sum((Decimal(e['bytes'])*self.rates[(e['source'],e['destination'])]
                    /Decimal(self.units[(e['source'],e['destination'])]) for e in edges),Decimal('0'))


def quantile_nearest(values,p):
    v=sorted(values)
    require(bool(v) and 0<p<=1,'bad percentile input')
    return v[max(0, math.ceil(len(v)*p)-1)]


def profile_plans(plans,tariffs,quantile=0.5):
    profiles={}
    for name, samples in plans.items():
        tr=[x for x in samples if x['trial'] in TRAIN_TRIALS]
        require(len(tr)==5,'Training split wrong')
        layouts=[[(e['source'],e['destination'],e['label']) for e in x['edges']] for x in tr]
        require(all(layout == layouts[0] for layout in layouts),'Plan edge layout changed across trials')
        pred_edges=[]
        for i,(src,dst,label) in enumerate(layouts[0]):
            pred_edges.append({'source':src,'destination':dst,'label':label,
                               'bytes':int(statistics.median([x['edges'][i]['bytes'] for x in tr]))})
        # Stage / post-stage are not independent network timings; sum is a workload-specific empirical predictor.
        predicted_stage=quantile_nearest([x['stage_s'] for x in tr],quantile)
        predicted_remainder=quantile_nearest([x['duration_s']-x['stage_s'] for x in tr],quantile)
        pred_time=predicted_stage+predicted_remainder
        # In particular, quantile=0.9 estimates a conservative *training* envelope, not a 90% statistical SLA guarantee.
        profiles[name]={'plan':name,'time_s':pred_time,'stage_s':predicted_stage,
                        'remainder_s':predicted_remainder,
                        'bytes':sum(e['bytes'] for e in pred_edges),
                        'edges':pred_edges, 'cost_usd_proxy':tariffs.cost(pred_edges),
                        'training_count':len(tr)}
    return profiles


def pareto(profiles):
    # Only terminal, result-equivalent, same-SLO-boundary *candidate plans*, not a relational memo DP.
    retained=[]
    for p in profiles.values():
        dominated=any((q['cost_usd_proxy']<=p['cost_usd_proxy'] and q['time_s']<=p['time_s'] and
                       (q['cost_usd_proxy']<p['cost_usd_proxy'] or q['time_s']<p['time_s']))
                      for q in profiles.values() if q['plan']!=p['plan'])
        if not dominated:retained.append(p)
    return sorted(retained,key=lambda x:(x['cost_usd_proxy'],x['time_s'],x['plan']))


def choose(profiles, slo_s, method='egressopt'):
    viable=[p for p in profiles.values() if p['time_s'] <= slo_s+1e-12]
    require(bool(viable),'No estimated latency-feasible plans')
    if method=='egressopt':return min(viable,key=lambda p:(p['cost_usd_proxy'],p['time_s'],p['plan']))
    if method=='bytes_only':return min(viable,key=lambda p:(p['bytes'],p['time_s'],p['plan']))
    if method=='latency_only':return min(viable,key=lambda p:(p['time_s'],p['cost_usd_proxy'],p['plan']))
    raise ValueError('Unknown selector method: '+method)


def collect_analysis(records,manifest,tariffs,deltas=(0,0.2,0.5,1.0),quantile=0.5):
    rows=[]; pred_error=[]; decisions=[]
    for q,plans in records.items():
        prof=profile_plans(plans,tariffs,quantile)
        fastest=min(p['time_s'] for p in prof.values())
        for plan, p in prof.items():
            ev=[x for x in plans[plan] if x['trial'] in TEST_TRIALS]
            test_mean=statistics.mean(x['duration_s'] for x in ev)
            test_cost=statistics.mean(float(tariffs.cost(x['edges'])) for x in ev)
            pred_error.append({'query':q,'plan':plan,'pred_ms':p['time_s']*1e3,
                               'test_mean_ms':test_mean*1e3,
                               'relative_error_pct':100*(p['time_s']-test_mean)/test_mean,
                               'calibration_trials':'1-5','evaluation_trials':'6-10'})
        for delta in deltas:
            slo=fastest*(1+delta)
            choice={m:choose(prof,slo,m) for m in ('egressopt','bytes_only','latency_only')}
            for method,sel in choice.items():
                ev=[x for x in plans[sel['plan']] if x['trial'] in TEST_TRIALS]
                test_mean=statistics.mean(x['duration_s'] for x in ev)
                test_max=max(x['duration_s'] for x in ev)
                violations=sum(x['duration_s']>slo for x in ev)
                actual_mean_cost=statistics.mean(float(tariffs.cost(x['edges'])) for x in ev)
                rows.append({'query':q,'delta':delta,'slo_ms':slo*1000,'selector':method,
                             'selected_plan':sel['plan'],'predicted_latency_ms':sel['time_s']*1000,
                             'measured_test_mean_ms':test_mean*1000,'measured_test_max_ms':test_max*1000,
                             'test_trial_slo_violations':violations,'test_trial_count':len(ev),
                             'predicted_http_bytes':sel['bytes'],
                             'predicted_cost_usd_proxy':float(sel['cost_usd_proxy']),
                             'test_mean_cost_usd_proxy':actual_mean_cost,
                             'scenario':tariffs.id})
            decisions.append({'query':q,'delta':delta,'threshold_ms':slo*1000,
                              'pareto_terminal_candidates':[p['plan'] for p in pareto(prof)],
                              'plan_choice':{m:p['plan'] for m,p in choice.items()},
                              'same_as_bytes_only':choice['egressopt']['plan']==choice['bytes_only']['plan']})
    return rows,pred_error,decisions


def write_csv(path,rows):
    require(bool(rows),'empty output CSV')
    with open(path,'w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)


def evaluate(args):
    start=time.perf_counter()
    t=Tariffs(args.tariffs)
    z=Archive(args.archive)
    records,manifest=z.load()
    rows,errors,decisions=collect_analysis(records,manifest,t,quantile=args.calibration_quantile)
    output=pathlib.Path(args.out)
    require(not output.exists(),'Output directory must not already exist: '+str(output))
    output.mkdir(parents=True)
    write_csv(output/'heldout_decisions.csv',rows)
    write_csv(output/'prediction_errors.csv',errors)
    write_csv(output/'plan_selection_details.csv',[
        {'query':d['query'],'delta':d['delta'],'threshold_ms':d['threshold_ms'],
         'terminal_pareto_candidates':';'.join(d['pareto_terminal_candidates']),
         **d['plan_choice'],'same_as_bytes_only':d['same_as_bytes_only']} for d in decisions])
    summary={'model':'empirical per-plan stage+post-stage training quantiles; no network time isolation',
             'algorithm':'finite validated candidate enumeration, Pareto terminal filtering, price/SLO selector',
             'dataset':'SF1/P1 six real-cloud query experiments',
             'evaluation_kind':'offline held-out replay, NOT prospective live optimizer evaluation',
             'calibration_trials':list(TRAIN_TRIALS),'evaluation_trials':list(TEST_TRIALS),
             'calibration_quantile':args.calibration_quantile,
             'scenario_id':t.id,'pricing_classification':t.data['classification'],
             'tariff_json_sha256':t.sha256,
             'optimizer_source_sha256':stable_hash(pathlib.Path(__file__).read_bytes()),
             'source_zip_sha256':stable_hash(z.path.read_bytes()),
             'selected_runs':manifest,'plans':sum(len(x) for x in records.values()),
             'measured_samples_loaded':sum(len(v) for d in records.values() for v in d.values()),
             'latency_prediction_mae_ms':statistics.mean(abs(x['pred_ms']-x['test_mean_ms']) for x in errors),
             'latency_prediction_mape_pct':statistics.mean(abs(x['relative_error_pct']) for x in errors),
             'evaluated_query_delta_pairs':len(decisions),
             'tariff_changes_plan_vs_bytes_only':sum(not x['same_as_bytes_only'] for x in decisions),
             'test_trial_slo_violations':{m:sum(r['test_trial_slo_violations'] for r in rows if r['selector']==m)
                                            for m in ('egressopt','bytes_only','latency_only')},
             'measured_billing_status':'NOT AVAILABLE; costs are HTTP payload x scenario rate',
             'excluded_costs':['compute','control plane','result delivery','network overhead','monthly tier allocation'],
             'limitations':['single placement and SF1','plan-specific, not transferable to unmeasured plans',
                            'no independent cloud prospective optimizer run','small holdout per plan (5)',
                            'not billing reconciliation','memo search kernel separate from six cloud runners']}
    (output/'audit_manifest.json').write_text(json.dumps(summary,indent=2,sort_keys=True)+'\n')
    print('EGRESSOPT_HELDOUT_REPLAY_PASS')
    print('queries',len(records),'plans',summary['plans'],'raw_trials',summary['measured_samples_loaded'])
    print('decision_cases',len(decisions),'tariff_vs_bytes_plan_differences',summary['tariff_changes_plan_vs_bytes_only'])
    print('latency MAPE (plan mean): %.2f%%'%summary['latency_prediction_mape_pct'])
    print('cost accounting: HTTP payload proxy under',t.id,'(NOT invoice dollars)')
    print('artifacts:',output.resolve())


def audit_evaluation(args):
    """Recompute held-out decisions, checking the source archive and tariff hashes."""
    folder=pathlib.Path(args.eval_dir)
    s=json.loads((folder/'audit_manifest.json').read_text())
    t=Tariffs(args.tariffs)
    a=Archive(args.archive)
    require(s['source_zip_sha256']==stable_hash(a.path.read_bytes()),'Archive ZIP changed after evaluation')
    require(s['tariff_json_sha256']==t.sha256,'Tariff configuration changed after evaluation')
    require(s['optimizer_source_sha256']==stable_hash(pathlib.Path(__file__).read_bytes()),
            'Optimizer code changed after evaluation; do not silently claim old artifacts were reproduced')
    records,manifest=a.load()
    require(s['selected_runs']==manifest,'Source trial/manifest files changed')
    rows,errors,details=collect_analysis(records,manifest,t,quantile=s['calibration_quantile'])
    comparison={
        'heldout_decisions.csv':rows,
        'prediction_errors.csv':errors,
        'plan_selection_details.csv':[
            {'query':d['query'],'delta':d['delta'],'threshold_ms':d['threshold_ms'],
             'terminal_pareto_candidates':';'.join(d['pareto_terminal_candidates']),
             **d['plan_choice'],'same_as_bytes_only':d['same_as_bytes_only']} for d in details]
    }
    for filename,recomputed in comparison.items():
        buffer=io.StringIO(newline='')
        w=csv.DictWriter(buffer,fieldnames=list(recomputed[0])); w.writeheader();w.writerows(recomputed)
        require((folder/filename).read_bytes()==buffer.getvalue().encode('utf-8'),
                filename+' differs from rederived values')
    print('EGRESSOPT_EVALUATION_AUDIT_PASS')
    print('Source, scenario, code and three analytical CSVs match exact recomputation.')


def select(args):
    t=Tariffs(args.tariffs)
    d,_=Archive(args.archive).load()
    q=args.query.upper()
    require(q in d,'Unknown query')
    p=profile_plans(d[q],t,args.calibration_quantile)
    slo=args.slo_ms/1000 if args.slo_ms is not None else min(v['time_s'] for v in p.values())*(1+args.delta)
    selections={m:choose(p,slo,m) for m in ('egressopt','bytes_only','latency_only')}
    result={'query':q,'measured_trial_count_not_used_for_selection':len(TEST_TRIALS)*len(p),
            'calibration_trials':list(TRAIN_TRIALS),'scenario':t.id,
            'scenario_classification':t.data['classification'],
            'slo_ms':slo*1000,'decisions':{m:v['plan'] for m,v in selections.items()},
            'candidate_plans':[{**{k:v for k,v in x.items() if k!='cost_usd_proxy'},
                                'cost_usd_proxy':str(x['cost_usd_proxy'])}
                               for x in sorted(p.values(),key=lambda v:v['plan'])],
            'terminal_pareto_frontier':[v['plan'] for v in pareto(p)],
            'interpretation':'Offline model decision only; no plan has been executed by this command.'}
    print(json.dumps(result,indent=2))


# A genuine but deliberately restricted connected-subset memo search kernel.
# Its inputs and cardinalities are *toy/synthetic*, not learned from the six cloud-run archives.
@dataclass(frozen=True)
class State:
    relations:frozenset
    site:str
    output_bytes:int
    signature:str

@dataclass
class Label:
    state:State
    cost:Decimal
    latency_s:float
    rows:int
    tree:str
    edges:tuple


def dominate(a,b):
    require(a.state==b.state,'Pareto dominance only for strictly equivalent output states')
    return (a.cost<=b.cost and a.latency_s<=b.latency_s and
            (a.cost<b.cost or a.latency_s<b.latency_s))


def insert_pareto(bucket, label):
    if any(x.cost==label.cost and abs(x.latency_s-label.latency_s)<1e-12 for x in bucket):return
    if any(dominate(x,label) for x in bucket):return
    bucket[:]=[x for x in bucket if not dominate(label,x)]
    bucket.append(label)


def memo_enumerate(toy,tariffs,prune=True):
    relation=toy['relations']; sites=toy['sites']; allowed={r:set(spec['available_sites']) for r,spec in relation.items()}
    link={frozenset(e) for e in toy['join_edges']}
    require(len(sites)>=2 and all(s in ALL_SITES for s in sites), 'toy sites invalid')
    require(all(len(x)==2 for x in link),'join edges must be pairs')
    join_rate=toy['join_rows_by_subset']; default_bw=float(toy['network_bytes_per_s']);
    setup=float(toy['network_setup_s']); join_cpu=float(toy['join_seconds_per_input_byte'])
    require(default_bw>0 and setup>=0 and join_cpu>=0,'invalid toy coefficients')
    memo=collections.defaultdict(list)
    for r,spec in sorted(relation.items()):
        byte=int(spec['projected_bytes']); rows=int(spec['rows'])
        require(byte>=0 and rows>0, 'bad relation stats')
        for site in sorted(allowed[r]):
            st=State(frozenset({r}),site,byte,toy.get('output_signature','unsorted-parquet'))
            memo[st].append(Label(st,Decimal('0'),float(spec.get('scan_s',0)),rows,r,()))
    names=frozenset(relation)
    def connected(rs):
        seen={next(iter(rs))}; rest=set(rs)-seen
        while rest:
            nexts={x for x in rest if any(frozenset((x,y)) in link for y in seen)}
            if not nexts:return False
            seen.update(nexts);rest-=nexts
        return True
    for size in range(2,len(names)+1):
        for combo in itertools.combinations(sorted(names),size):
            whole=frozenset(combo)
            if not connected(whole):continue
            canonical=list(combo)
            for a_size in range(1,size):
                for a_tuple in itertools.combinations(canonical,a_size):
                    a=frozenset(a_tuple);b=whole-a
                    if min(a)>min(b):continue  # deterministic unordered partition; no duplicates
                    if not (connected(a) and connected(b)):continue
                    if not any(frozenset((x,y)) in link for x in a for y in b):continue
                    left=[x for key,vals in memo.items() if key.relations==a for x in vals]
                    right=[x for key,vals in memo.items() if key.relations==b for x in vals]
                    for l,r,site in itertools.product(left,right,sites):
                        # User supplies output bytes, avoiding unvalidated cardinality independence assumptions.
                        out_key=','.join(sorted(whole))
                        require(out_key in join_rate,'Missing output stats for connected subset '+out_key)
                        rows=int(join_rate[out_key]['rows']); bytes_out=int(join_rate[out_key]['bytes'])
                        st=State(whole,site,bytes_out,toy.get('output_signature','unsorted-parquet'))
                        new_edges=list(l.edges+r.edges)
                        cost=l.cost+r.cost
                        completion=[]
                        for child in (l,r):
                            transfer_s=0.0
                            if child.state.site!=site:
                                e={'source':child.state.site,'destination':site,'bytes':child.state.output_bytes,'label':'toy_exchange'}
                                transfer_s=setup+child.state.output_bytes/default_bw
                                cost+=tariffs.cost([e]);new_edges.append(e)
                            completion.append(child.latency_s+transfer_s)
                        latency=max(completion)+join_cpu*(l.state.output_bytes+r.state.output_bytes)
                        label=Label(st,cost,latency,rows,
                                    f'JOIN@{site}({l.tree},{r.tree})',tuple(new_edges))
                        if prune:insert_pareto(memo[st],label)
                        else:memo[st].append(label)
    final_site=toy['final_site']
    # A root join may occur at any site; deliver its final result to a fixed coordinator.
    # This models result delivery in the SYNTHETIC memo only. The real SF1 archive's
    # HTTP measurement boundary omits final result delivery to the controller.
    raw_roots=[x for k,vs in memo.items() if k.relations==names for x in vs]
    final_labels=[]
    for root in raw_roots:
        if root.state.site==final_site:
            final_labels.append(root)
            continue
        edge={'source':root.state.site,'destination':final_site,
              'bytes':root.state.output_bytes,'label':'toy_final_delivery'}
        dest_state=State(names,final_site,root.state.output_bytes,root.state.signature)
        transferred=Label(dest_state,root.cost+tariffs.cost([edge]),
                          root.latency_s+setup+root.state.output_bytes/default_bw,
                          root.rows,root.tree+f' -> DELIVER@{final_site}',
                          root.edges+(edge,))
        if prune:insert_pareto(final_labels,transferred)
        else:final_labels.append(transferred)
    # Destination-local roots participate in the same terminal Pareto frontier.
    if prune:
        terminal=[]
        for root in final_labels:insert_pareto(terminal,root)
        final_labels=terminal
    require(bool(final_labels),'no final-site alternatives')
    fastest=min(x.latency_s for x in final_labels)
    threshold=fastest*(1+float(toy['max_slowdown_fraction']))
    feasible=[x for x in final_labels if x.latency_s<=threshold+1e-12]
    require(bool(feasible),'no feasible toy plan')
    winner=min(feasible,key=lambda x:(x.cost,x.latency_s,x.tree))
    return {'states':len(memo),'labels':sum(len(x) for x in memo.values()),
            'full_plans_at_final_site':len(final_labels),'fastest_estimated_s':fastest,
            'slo_s':threshold,'winner':winner}


def memo_demo(args):
    tariff=Tariffs(args.tariffs)
    toy=json.loads(pathlib.Path(args.toy).read_text())
    exact=memo_enumerate(toy,tariff,prune=False)
    pruned=memo_enumerate(toy,tariff,prune=True)
    require(exact['winner'].cost==pruned['winner'].cost and
            abs(exact['winner'].latency_s-pruned['winner'].latency_s)<1e-9,
            'Toy Pareto pruning changed optimal terminal cost/latency')
    print(json.dumps({'label':'SYNTHETIC MODEL UNIT TEST; NOT MEASURED AWS/AZURE/GCP PERFORMANCE',
                      'baseline_exhaustive_final_labels':exact['full_plans_at_final_site'],
                      'pareto_final_labels':pruned['full_plans_at_final_site'],
                      'candidate_states':pruned['states'],'retained_labels':pruned['labels'],
                      'selected_join_tree':pruned['winner'].tree,
                      'modeled_cost_proxy_usd':str(pruned['winner'].cost),
                      'modeled_latency_ms':1000*pruned['winner'].latency_s,
                      'memo_pruning_consistent_with_unpruned_enumeration':True},indent=2))
    print('EGRESSOPT_MEMO_TOY_PASS')


def selfcheck():
    import tempfile
    with tempfile.TemporaryDirectory() as directory:
        p=pathlib.Path(directory)/'rates.json'
        p.write_text(json.dumps({'classification':'ILLUSTRATIVE_SCENARIO_NOT_VERIFIED_RATES',
                  'unit_bytes':1000,'tier_assumption':'fixed_marginal_rates_no_free_tier',
                  'scenario_id':'test','directed_rates':[{'source':a,'destination':b,'usd_per_unit': '0.1'}
                                      for a in ALL_SITES for b in ALL_SITES if a!=b]}))
        t=Tariffs(p)
        e=[{'source':'aws','destination':'azure','bytes':250,'label':'x'}]
        require(t.cost(e)==Decimal('0.025'),'directional rate failed')
        sample={'a':{'plan':'a','time_s':0.2,'cost_usd_proxy':Decimal('.010'),'bytes':100},
                'b':{'plan':'b','time_s':0.1,'cost_usd_proxy':Decimal('.020'),'bytes':50},
                'c':{'plan':'c','time_s':0.25,'cost_usd_proxy':Decimal('.030'),'bytes':99}}
        require([x['plan'] for x in pareto(sample)]==['a','b'],'Pareto filtering failed')
        require(choose(sample,0.21)['plan']=='a','cost/SLO selection failed')
        require(choose(sample,0.1)['plan']=='b','tight-SLO selection failed')
        require(choose(sample,0.21,'bytes_only')['plan']=='b','bytes-only selection failed')
        try:choose(sample,0.01)
        except ValueError:pass
        else:raise AssertionError('infeasible SLO not rejected')
        # Test the six direction pairs and unsigned edge constraints.
        for a in ALL_SITES:
            for b in ALL_SITES:
                if a!=b: require(t.cost([{'source':a,'destination':b,'bytes':1}])>0,'pair missing')
    print('EGRESSOPT_SELFCHECK_PASS (directional pricing, SLO, Pareto, invalid-SLO checks)')


def main(argv=None):
    ap=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    sp=ap.add_subparsers(dest='mode',required=True)
    sp.add_parser('selfcheck')
    for cmd in ('evaluate','select'):
        p=sp.add_parser(cmd);p.add_argument('--archive',required=True)
        p.add_argument('--tariffs',required=True)
        p.add_argument('--calibration-quantile',type=float,choices=(0.5,0.8,1.0),default=0.5,
                       help='0.5 median, 0.8 fourth-of-five, 1 max-of-five training envelope')
        if cmd=='evaluate':p.add_argument('--out',default='egressopt_eval')
        else:
            p.add_argument('--query',required=True,choices=list(RUNS))
            p.add_argument('--slo-ms',type=float,help='Absolute predicted latency threshold in milliseconds')
            p.add_argument('--delta',type=float,default=0.2,
                           help='If no SLO, fraction above fastest predicted plan (default 0.2)')
    p=sp.add_parser('audit-evaluation');p.add_argument('--archive',required=True);p.add_argument('--tariffs',required=True);p.add_argument('--eval-dir',required=True)
    p=sp.add_parser('memo-demo');p.add_argument('--tariffs',required=True);p.add_argument('--toy',required=True)
    args=ap.parse_args(argv)
    if args.mode=='selfcheck':selfcheck()
    elif args.mode=='evaluate':evaluate(args)
    elif args.mode=='select':select(args)
    elif args.mode=='memo-demo':memo_demo(args)
    elif args.mode=='audit-evaluation':audit_evaluation(args)
    return 0

if __name__=='__main__':
    try:sys.exit(main())
    except (ValueError,FileNotFoundError,KeyError,zipfile.BadZipFile) as err:
        print('EGRESSOPT_ERROR:',err,file=sys.stderr);sys.exit(2)
