# -*- coding: utf-8 -*-
"""
BURST_RADAR_CALENDAR daily refresh (worker para V2 PERMA MASTER DAILY PIPELINE).

Hace 3 cosas:
  1. SURFACE INCREMENTAL: detecta 30MINDATA_*.csv mas nuevos que el max(date) del
     parquet surface_1030_daily_store.parquet, extrae el snapshot 10:30 (misma logica
     que analysis/build_calendar_surface_1030_store.py, embebida aqui para ser
     autocontenido) y APPENDEA al parquet. Valida calidad (min filas snapshot).
  2. RECOMPUTE RADAR_LITE: CAL_RATIO_25_C = mean(iv_25 calls DTE 25-35) / mean(iv_25
     calls DTE 38-52) por dia -> percentil expanding (warmup 252) -> RADAR = (1-pct)*100.
     Actualiza el CSV canonico BURST_RADAR_LITE_DAILY.csv del estudio.
  3. data.json: actualiza 'series' + 'latest' + 'meta.chart_*' y git push SSH.

Exit codes (compatibles con run_dashboard_generic de V2):
  0 = data.json actualizado y pusheado
  3 = sin cambios (idempotente, no commit)
  2 = warn de datos (fuente vacia/incompleta/dia parcial)
  1 = error
"""
import sys, os, json, re, shutil, subprocess
from datetime import datetime
from pathlib import Path
from bisect import bisect_right, insort
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')

DIR  = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(DIR, 'data.json')
PARQUET   = r'C:/Users/Administrator/Desktop/Backtests DATABASE/Calendar SPX/ANALISIS/CQI_V4_NEXT_LEVEL/surface_1030_daily_store.parquet'
CHAIN_DIR = r'C:/Users/Administrator/Desktop/FINAL DATA/HIST AND STREAMING DATA/UPDATED HISTORICAL DAYS'
SPX_PATH  = r'C:/Users/Administrator/Desktop/FINAL DATA/SP_SPX_CLOSE_HISTORICAL_PRICES.csv'
LITE_CSV  = r'C:/Users/Administrator/Desktop/BULK OPTIONSTRAT/ESTRATEGIAS/Calendar/ANALISIS/BURST_RADAR_20260610/BURST_RADAR_LITE_DAILY.csv'

SNAPSHOT_HHMM = '10:30'
FRONT_BAND = (25, 35)
BACK_BAND  = (38, 52)
WARMUP = 252
MIN_SNAP_ROWS = 1000   # dia parcial si el snapshot 10:30 tiene menos filas
FILE_RE = re.compile(r'^30MINDATA_(\d{4}-\d{2}-\d{2})\.csv$', re.I)
REQUIRED = ['date','ms_of_day','underlying_price','expiration','right','strike',
            'bid','ask','mid','delta','implied_vol']
OPTIONAL = ['IV_BS']   # fallback cuando implied_vol viene vacio (streaming post 2026-04)
IV_SENTINEL_MIN = 0.01 # IV_BS sentinel 0.0001 = sin calidad -> tratar como NaN

def log(m): print(f"[BURST-RADAR {datetime.now():%H:%M:%S}] {m}", flush=True)

# ============================================================
# Surface extraction (embebido de analysis/build_calendar_surface_1030_store.py)
# ============================================================
def normalize_right(x):
    v = str(x).strip().upper()
    if v in ('C','CALL'): return 'C'
    if v in ('P','PUT'): return 'P'
    return None

def nearest_time_rows(df, hhmm):
    target = pd.to_datetime(hhmm, format='%H:%M', errors='coerce')
    target_min = target.hour * 60 + target.minute
    t = pd.to_datetime(df['ms_of_day'], format='%H:%M:%S.%f', errors='coerce')
    mins = t.dt.hour * 60 + t.dt.minute
    if mins.notna().sum() == 0:
        return df.iloc[0:0].copy()
    diff = (mins - target_min).abs()
    return df[diff == diff.min()].copy()

def pick_iv_by_delta_or_moneyness(g, target_abs_delta, right_norm, spot):
    d = pd.to_numeric(g['delta'], errors='coerce').abs()
    iv = pd.to_numeric(g['implied_vol'], errors='coerce')
    if d.notna().sum() > 0:
        idx = (d - target_abs_delta).abs().idxmin()
        v = iv.loc[idx]
        if pd.notna(v): return float(v)
    strike = pd.to_numeric(g['strike'], errors='coerce')
    mny = (strike / spot) - 1.0
    if right_norm == 'C':
        target_mny = 0.03 if abs(target_abs_delta - 0.25) < 1e-9 else 0.08
    else:
        target_mny = -0.03 if abs(target_abs_delta - 0.25) < 1e-9 else -0.08
    idx = (mny - target_mny).abs().idxmin()
    v = iv.loc[idx]
    return float(v) if pd.notna(v) else np.nan

def compute_surface_rows_for_file(path, snapshot_hhmm=SNAPSHOT_HHMM):
    """Identica a build_calendar_surface_1030_store.compute_surface_rows_for_file.
    Devuelve (rows, n_snap_rows)."""
    try:
        wanted = set(REQUIRED + OPTIONAL)
        df = pd.read_csv(path, usecols=lambda c: c in wanted, low_memory=False)
    except Exception as exc:
        log(f"WARN read failed {Path(path).name}: {exc}")
        return [], 0
    snap = nearest_time_rows(df, snapshot_hhmm)
    n_snap = len(snap)
    if snap.empty: return [], 0
    snap['date_us'] = pd.to_datetime(snap['date'], errors='coerce').dt.normalize()
    snap['expiration'] = pd.to_datetime(snap['expiration'], errors='coerce').dt.normalize()
    snap['right_norm'] = snap['right'].map(normalize_right)
    num_cols = ['underlying_price','strike','bid','ask','mid','delta','implied_vol']
    if 'IV_BS' in snap.columns: num_cols.append('IV_BS')
    for c in num_cols:
        snap[c] = pd.to_numeric(snap[c], errors='coerce')
    # Fallback: implied_vol vacio (streaming post 2026-04) -> usar IV_BS local
    if 'IV_BS' in snap.columns:
        snap['implied_vol'] = snap['implied_vol'].fillna(snap['IV_BS'])
    # Sentinel guard: IV degenerada (< 0.01, ej. 0.0001) = sin calidad -> NaN
    snap.loc[snap['implied_vol'] < IV_SENTINEL_MIN, 'implied_vol'] = np.nan
    snap = snap.dropna(subset=['date_us','expiration','right_norm','underlying_price','strike','mid','implied_vol'])
    if snap.empty: return [], n_snap
    snap['dte_days'] = (snap['expiration'] - snap['date_us']).dt.days
    snap = snap[(snap['dte_days'] >= 1) & (snap['dte_days'] <= 450)]
    if snap.empty: return [], n_snap
    rows = []
    for (d, r, dte), g in snap.groupby(['date_us','right_norm','dte_days'], sort=False):
        if g.empty: continue
        spot = float(pd.to_numeric(g['underlying_price'], errors='coerce').median())
        if not np.isfinite(spot) or spot <= 0: continue
        strike = pd.to_numeric(g['strike'], errors='coerce')
        iv = pd.to_numeric(g['implied_vol'], errors='coerce')
        mid = pd.to_numeric(g['mid'], errors='coerce')
        bid = pd.to_numeric(g['bid'], errors='coerce')
        ask = pd.to_numeric(g['ask'], errors='coerce')
        mny = (strike / spot) - 1.0
        i_atm = mny.abs().idxmin()
        iv_atm = iv.loc[i_atm] if i_atm in iv.index else np.nan
        mid_atm = mid.loc[i_atm] if i_atm in mid.index else np.nan
        bid_atm = bid.loc[i_atm] if i_atm in bid.index else np.nan
        ask_atm = ask.loc[i_atm] if i_atm in ask.index else np.nan
        spread_rel_atm = np.nan
        if pd.notna(mid_atm) and mid_atm > 0 and pd.notna(bid_atm) and pd.notna(ask_atm):
            spread_rel_atm = float((ask_atm - bid_atm) / mid_atm)
        iv25 = pick_iv_by_delta_or_moneyness(g, 0.25, r, spot)
        iv10 = pick_iv_by_delta_or_moneyness(g, 0.10, r, spot)
        skew = float(iv25 - iv_atm) if (pd.notna(iv25) and pd.notna(iv_atm)) else np.nan
        convex = float(iv10 - 2.0*iv25 + iv_atm) if (pd.notna(iv10) and pd.notna(iv25) and pd.notna(iv_atm)) else np.nan
        iv_near = iv[(mny.abs() <= 0.05) & iv.notna()]
        iv_disp = float(iv_near.std(ddof=0)) if len(iv_near) >= 2 else np.nan
        rows.append({
            'date_us': pd.Timestamp(d).strftime('%Y-%m-%d'), 'right_norm': r, 'dte_days': int(dte),
            'underlying_price': spot,
            'iv_atm': float(iv_atm) if pd.notna(iv_atm) else np.nan,
            'iv_25': float(iv25) if pd.notna(iv25) else np.nan,
            'iv_10': float(iv10) if pd.notna(iv10) else np.nan,
            'skew_25_atm': skew, 'convex_10_25_atm': convex,
            'spread_rel_atm': spread_rel_atm, 'iv_dispersion_5pct': iv_disp,
            'n_quotes': int(g['mid'].notna().sum()), 'source_file': Path(path).name,
        })
    return rows, n_snap

# ============================================================
# 1. Surface incremental
# ============================================================
def update_surface_incremental():
    """Appendea dias FALTANTES al parquet (no solo > max: tambien huecos intermedios
    de los ultimos 180 dias, p.ej. dias saltados por datos parciales que luego se
    completaron). Devuelve (n_appended, n_partial_skipped)."""
    surf = pd.read_parquet(PARQUET)
    existing = set(pd.to_datetime(surf['date_us']).dt.normalize())
    max_date = max(existing)
    floor = max_date - pd.Timedelta(days=180)
    log(f"parquet max date: {max_date.date()} ({len(surf):,} rows)")

    new_files = []
    for p in sorted(Path(CHAIN_DIR).glob('30MINDATA_*.csv')):
        m = FILE_RE.match(p.name)
        if not m: continue
        dt = pd.to_datetime(m.group(1), errors='coerce')
        if pd.isna(dt): continue
        dt = dt.normalize()
        if dt in existing: continue
        if dt <= max_date and dt < floor: continue  # huecos solo en ventana 180d
        new_files.append((dt, p))
    if not new_files:
        log("surface al dia (sin 30MINDATA nuevos)")
        return 0, 0

    log(f"30MINDATA nuevos: {len(new_files)} ({new_files[0][0].date()} -> {new_files[-1][0].date()})")
    all_rows = []; n_partial = 0
    for i, (dt, p) in enumerate(new_files, 1):
        rows, n_snap = compute_surface_rows_for_file(p)
        if n_snap < MIN_SNAP_ROWS or not rows:
            log(f"  SKIP dia parcial {p.name} (snap rows={n_snap}, surface rows={len(rows)})")
            n_partial += 1
            continue
        all_rows.extend(rows)
        if i % 10 == 0 or i == len(new_files):
            log(f"  procesados {i}/{len(new_files)} ({len(all_rows)} rows)")
    if not all_rows:
        return 0, n_partial

    new_df = pd.DataFrame(all_rows)
    combined = pd.concat([surf, new_df], ignore_index=True)
    combined = combined.sort_values(['date_us','right_norm','dte_days'], kind='mergesort').reset_index(drop=True)
    tmp = PARQUET + '.tmp'
    combined.to_parquet(tmp, index=False)
    shutil.move(tmp, PARQUET)
    log(f"parquet appended: +{new_df['date_us'].nunique()} dias ({len(new_df)} rows) -> total {len(combined):,}")
    return new_df['date_us'].nunique(), n_partial

# ============================================================
# 2. Recompute RADAR_LITE
# ============================================================
def _band_mean_robust(g):
    """Media de iv_25 con filtro de calidad: descarta expiraciones cuya iv_25 se
    desvia mas de 2x (o menos de 0.5x) de la MEDIANA de la banda ese dia.
    Motivo: expiraciones con quotes basura (ej. 2026-05-25 festivo, DTE45 iv=1.29
    vs 0.127 vecinas) contaminaban la media y producian RADAR espureo."""
    v = g.dropna()
    if len(v) == 0: return np.nan
    med = v.median()
    if med <= 0: return np.nan
    clean = v[(v >= 0.5 * med) & (v <= 2.0 * med)]
    return clean.mean() if len(clean) else np.nan

def compute_radar_series():
    """CAL_RATIO_25_C por dia -> expanding pct -> RADAR_LITE. Devuelve DataFrame."""
    surf = pd.read_parquet(PARQUET, columns=['date_us','right_norm','dte_days','iv_25'])
    surf = surf[surf['right_norm'] == 'C'].copy()
    surf['date'] = pd.to_datetime(surf['date_us'])
    front = surf[(surf['dte_days'] >= FRONT_BAND[0]) & (surf['dte_days'] <= FRONT_BAND[1])]
    back  = surf[(surf['dte_days'] >= BACK_BAND[0])  & (surf['dte_days'] <= BACK_BAND[1])]
    f = front.groupby('date')['iv_25'].apply(_band_mean_robust).rename('F_iv_25_C')
    b = back.groupby('date')['iv_25'].apply(_band_mean_robust).rename('B_iv_25_C')
    df = pd.concat([f, b], axis=1).dropna().sort_index()
    df['CAL_RATIO_25_C'] = df['F_iv_25_C'] / df['B_iv_25_C']

    vals = df['CAL_RATIO_25_C'].values
    pct = np.full(len(vals), np.nan)
    hist = []
    for i, v in enumerate(vals):
        if not np.isfinite(v): continue
        if len(hist) >= WARMUP:
            pct[i] = bisect_right(hist, float(v)) / len(hist)
        insort(hist, float(v))
    df['RADAR_LITE'] = (1 - pct) * 100
    df = df.reset_index()
    out = df.dropna(subset=['RADAR_LITE']).copy()
    out['SEMAFORO'] = pd.cut(out['RADAR_LITE'], bins=[-0.1, 20, 80, 100.1], labels=['ROJO','AMBAR','VERDE'])
    return out

def banda(p):
    if p >= 80: return 'VERDE'
    if p <= 20: return 'ROJO'
    return 'AMBAR'

def git(args):
    return subprocess.run(['git','-C',DIR]+args, capture_output=True, text=True)

# ============================================================
# main
# ============================================================
def main():
    try:
        if not os.path.isfile(DATA):
            log(f"data.json no existe -> regenera primero con update_dashboard.py"); return 1

        n_new, n_partial = update_surface_incremental()

        radar = compute_radar_series()
        if len(radar) < 300:
            log(f"serie RADAR insuficiente (N={len(radar)})"); return 2
        log(f"RADAR series: {len(radar):,} dias ({radar['date'].min().date()} -> {radar['date'].max().date()})")

        # CSV canonico vivo
        try:
            radar[['date','RADAR_LITE','SEMAFORO','CAL_RATIO_25_C']].to_csv(LITE_CSV, index=False)
        except Exception as e:
            log(f"WARN no pude escribir CSV canonico: {e}")

        # SPX overlay
        spx = pd.read_csv(SPX_PATH, usecols=['time','close'])
        spx['date'] = pd.to_datetime(spx['time']).dt.normalize()
        radar = radar.merge(spx[['date','close']].rename(columns={'close':'spx'}), on='date', how='left')

        data = json.load(open(DATA, encoding='utf-8'))
        series = [{'t': r['date'].strftime('%Y-%m-%d'),
                   'radar': round(float(r['RADAR_LITE']), 2),
                   'spx': (round(float(r['spx']), 2) if pd.notna(r['spx']) else None)}
                  for _, r in radar.iterrows()]
        last = radar.iloc[-1]
        reg = banda(float(last['RADAR_LITE']))
        # dias consecutivos en el regimen actual
        days_in = 1
        regs = [banda(v) for v in radar['RADAR_LITE'].values]
        for k in range(len(regs)-2, -1, -1):
            if regs[k] == reg: days_in += 1
            else: break
        latest = {'date': last['date'].strftime('%Y-%m-%d'),
                  'radar': float(round(last['RADAR_LITE'], 1)),
                  'regime': reg,
                  'cal_ratio': float(round(last['CAL_RATIO_25_C'], 4)),
                  'iv_front': float(round(last['F_iv_25_C']*100, 2)),
                  'iv_back': float(round(last['B_iv_25_C']*100, 2)),
                  'days_in_regime': int(days_in)}
        data['series'] = series
        data['latest'] = latest
        data.setdefault('meta', {})['chart_date_max'] = latest['date']
        data['meta']['chart_n_days'] = int(len(series))
        json.dump(data, open(DATA, 'w', encoding='utf-8'), indent=2)
        log(f"data.json patched: {len(series)} dias, latest={latest['date']} RADAR={latest['radar']:.1f} ({reg})")

        # git push
        git(['add','data.json'])
        if git(['diff','--cached','--quiet']).returncode == 0:
            log("sin cambios en data.json -> idempotente"); return 3
        c = git(['-c','user.email=noreply@anthropic.com','-c','user.name=manumartinb',
                 'commit','-m', f"daily radar refresh {latest['date']}"])
        if c.returncode != 0:
            log(f"commit fallo: {c.stderr.strip()}"); return 1
        p = git(['push','origin','main'])
        if p.returncode != 0:
            log(f"push fallo: {p.stderr.strip()}"); return 1
        log("pushed -> https://manumartinb.github.io/BURST_RADAR_CALENDAR/")
        if n_partial > 0:
            log(f"NOTA: {n_partial} dia(s) parciales saltados (se reintentan manana)"); return 2
        return 0
    except Exception as e:
        import traceback; traceback.print_exc()
        log(f"ERROR {type(e).__name__}: {e}"); return 1

if __name__ == '__main__':
    sys.exit(main())
