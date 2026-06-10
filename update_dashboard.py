# -*- coding: utf-8 -*-
"""
BURST_RADAR_CALENDAR dashboard - generador FULL (corre 1 vez + manual tras cambios).

Genera data.json completo:
  - latest + series (delegado a daily_refresh.compute_radar_series)
  - stats frozen del estudio APR (tabla 11/11, eventos, day-level honesto)
  - deciles + LOYO + semaforo tables (recomputados del target frozen del estudio)
NO pushea (eso lo hace daily_refresh.py o git manual).
"""
import sys, os, json
from datetime import datetime
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, DIR)
import daily_refresh as dr

DATA = os.path.join(DIR, 'data.json')
T1_CSV = r'C:/Users/Administrator/Desktop/BULK OPTIONSTRAT/ESTRATEGIAS/Calendar/ANALISIS/BURST_RADAR_20260610/burst_pure_target_T1_trade.csv'

def log(m): print(f"[BURST-FULL {datetime.now():%H:%M:%S}] {m}", flush=True)

def main():
    # 1. Serie viva
    radar = dr.compute_radar_series()
    log(f"RADAR series: {len(radar):,} dias")

    spx = pd.read_csv(dr.SPX_PATH, usecols=['time','close'])
    spx['date'] = pd.to_datetime(spx['time']).dt.normalize()
    radar_m = radar.merge(spx[['date','close']].rename(columns={'close':'spx'}), on='date', how='left')

    series = [{'t': r['date'].strftime('%Y-%m-%d'),
               'radar': round(float(r['RADAR_LITE']), 2),
               'spx': (round(float(r['spx']), 2) if pd.notna(r['spx']) else None)}
              for _, r in radar_m.iterrows()]
    last = radar_m.iloc[-1]
    reg = dr.banda(float(last['RADAR_LITE']))
    regs = [dr.banda(v) for v in radar_m['RADAR_LITE'].values]
    days_in = 1
    for k in range(len(regs)-2, -1, -1):
        if regs[k] == reg: days_in += 1
        else: break
    latest = {'date': last['date'].strftime('%Y-%m-%d'),
              'radar': float(round(last['RADAR_LITE'], 1)), 'regime': reg,
              'cal_ratio': float(round(last['CAL_RATIO_25_C'], 4)),
              'iv_front': float(round(last['F_iv_25_C']*100, 2)),
              'iv_back': float(round(last['B_iv_25_C']*100, 2)),
              'days_in_regime': int(days_in)}

    # 2. Frozen study tables (target frozen del estudio, 2020-2025)
    t1 = pd.read_csv(T1_CSV)
    t1['entry_dt'] = pd.to_datetime(t1['entry_dt'])
    t1 = t1.dropna(subset=['burst_pure_h15'])
    df = t1.merge(radar[['date','RADAR_LITE']], left_on='entry_dt', right_on='date', how='inner')
    df = df.dropna(subset=['RADAR_LITE','PnL_d015_mediana'])
    log(f"frozen join: {len(df):,} trades")

    from scipy.stats import spearmanr
    r_pool, _ = spearmanr(df['RADAR_LITE'], df['burst_pure_h15'])

    # Deciles
    df['DEC'] = pd.qcut(df['RADAR_LITE'].rank(method='first'), 10, labels=False) + 1
    deciles = []
    for d in range(1, 11):
        s = df[df['DEC'] == d]
        deciles.append({'dec': d, 'n': int(len(s)),
                        'bp_mean': float(round(s['burst_pure_h15'].mean(), 2)),
                        'wr': float(round((s['burst_pure_h15'] > 0).mean()*100, 1)),
                        'pnl15': float(round(s['PnL_d015_mediana'].mean(), 2))})

    # LOYO
    df['year'] = df['entry_dt'].dt.year
    loyo = []
    for y in sorted(df['year'].unique()):
        sy = df[df['year'] == y]
        if len(sy) < 50: continue
        ry, _ = spearmanr(sy['RADAR_LITE'], sy['burst_pure_h15'])
        loyo.append({'year': int(y), 'n': int(len(sy)), 'r': float(round(ry, 3))})

    # Semaforo trade-level + day-level honesto
    df['color'] = pd.cut(df['RADAR_LITE'], bins=[-0.1, 20, 80, 100.1], labels=['ROJO','AMBAR','VERDE'])
    sem_trade = []
    for c in ['VERDE','AMBAR','ROJO']:
        s = df[df['color'] == c]
        if len(s) == 0: continue
        sem_trade.append({'color': c, 'n': int(len(s)),
                          'bp_mean': float(round(s['burst_pure_h15'].mean(), 2)),
                          'hit': float(round((s['burst_pure_h15'] > 0).mean()*100, 1)),
                          'pnl15': float(round(s['PnL_d015_mediana'].mean(), 2))})
    dd = df.groupby('entry_dt').agg(pnl=('PnL_d015_mediana','mean'), bp=('burst_pure_h15','mean'),
                                     radar=('RADAR_LITE','first')).reset_index()
    dd['color'] = pd.cut(dd['radar'], bins=[-0.1, 20, 80, 100.1], labels=['ROJO','AMBAR','VERDE'])
    sem_day = []
    for c in ['VERDE','AMBAR','ROJO']:
        s = dd[dd['color'] == c]
        if len(s) == 0: continue
        sem_day.append({'color': c, 'n_days': int(len(s)),
                        'bp_day': float(round(s['bp'].mean(), 2)),
                        'pnl_day': float(round(s['pnl'].mean(), 2))})

    # APR frozen (resultados del estudio 2026-06-10, p9_radar_lite_final.py)
    apr = [
        {'code':'A1','test':'r_pool >= 0.15','result':'+0.3902','ok':True},
        {'code':'A3','test':'CI95 boot-by-day excluye 0','result':'[+0.314, +0.464]','ok':True},
        {'code':'A4','test':'permutation p < 0.01','result':'0.0000','ok':True},
        {'code':'A5','test':'tau deciles > 0.5','result':'+0.867','ok':True},
        {'code':'A6','test':'LOYO >= 5/6','result':'6/6 (2020-2025)','ok':True},
        {'code':'A7','test':'walk-forward >= 3/5','result':'5/5','ok':True},
        {'code':'B1','test':'partial_r > 0.05 (ctrl Z50/BBW/GEX/HV20/IV30)','result':'+0.3127','ok':True},
        {'code':'C1','test':'P90 strong-burst lift >= 1.5x','result':'1.96x','ok':True},
        {'code':'C2','test':'drift sin inversion de signo','result':'+0.410 / +0.356','ok':True},
        {'code':'C3','test':'OOS 2024+ > 0.20','result':'+0.4085','ok':True},
        {'code':'C5','test':'PnL VERDE > ROJO (p<0.05)','result':'p=0.0000','ok':True},
    ]
    events = {'n_events': 36, 'n_hits': 22, 'hit_pct': 61,
              'median_pre_burst': 67.2, 'median_control': 40.2, 'mw_p': '< 0.00001'}

    data = {
        'meta': {'date_min': series[0]['t'], 'date_max': series[-1]['t'],
                 'chart_date_max': latest['date'], 'chart_n_days': len(series),
                 'study_date': '2026-06-10', 'r_pool_frozen': float(round(r_pool, 4)),
                 'n_trades_frozen': int(len(df))},
        'latest': latest,
        'series': series,
        'apr': apr,
        'deciles': deciles,
        'loyo': loyo,
        'sem_trade': sem_trade,
        'sem_day': sem_day,
        'events': events,
    }
    json.dump(data, open(DATA, 'w', encoding='utf-8'), indent=2)
    log(f"data.json written: {os.path.getsize(DATA)/1024:.0f} KB | latest {latest['date']} RADAR={latest['radar']:.1f} ({reg})")
    return 0

if __name__ == '__main__':
    sys.exit(main())
