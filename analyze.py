import json, pandas as pd
from pathlib import Path

REPO_DIR  = Path(__file__).resolve().parent
DATA_DIR  = REPO_DIR / "data"
HOURLY    = sorted(DATA_DIR.glob("1h-*.json"))[-6:]      # last 6 snapshots
LATEST    = DATA_DIR / "latest.json"
MAPPING   = DATA_DIR / "mapping.json"

# ---------- helpers ----------------------------------------------------------
def load_mapping(fp: Path) -> pd.DataFrame:
    """mapping.json  ➜  id, name, limit"""
    with fp.open() as f:
        mapping = json.load(f)              # list[dict]
    return (pd.DataFrame(mapping)
              .set_index("id")[["name", "limit"]])

def load_latest(fp: Path) -> pd.DataFrame:
    """latest.json ➜ cur_high / cur_low"""
    with fp.open() as f:
        latest = json.load(f)["data"]       # dict[id → {...}]
    df = pd.DataFrame.from_dict(latest, orient="index")
    return df.rename(columns={"high": "cur_high", "low": "cur_low"})[["cur_high","cur_low"]]

def load_hourly(fp: Path) -> pd.DataFrame:
    """1h-*.json ➜ avgHighPrice / avgLowPrice / volumes + timestamp"""
    with fp.open() as f:
        obj = json.load(f)
    df = pd.DataFrame.from_dict(obj["data"], orient="index")
    df["timestamp"] = obj.get("timestamp", int(fp.stem.split("-")[1]))
    return df

# ---------- 1) read everything ----------------------------------------------
mapping_df = load_mapping(MAPPING)
latest_df  = load_latest(LATEST)
latest_df.index = latest_df.index.astype(int)            # id str ➜ int

hourlies   = pd.concat([load_hourly(fp) for fp in HOURLY])
hourlies.index = hourlies.index.astype(int)

hourly_agg = (hourlies
              .groupby(level=0)
              .agg(avgHighPrice = ("avgHighPrice",  "mean"),
                   avgLowPrice  = ("avgLowPrice",   "mean"),
                   highVol      = ("highPriceVolume","sum"),
                   lowVol       = ("lowPriceVolume", "sum")))

# ---------- 2) merge & compute metrics --------------------------------------
df = (latest_df
        .join(hourly_agg, how="left")
        .join(mapping_df, how="left")
        .dropna(subset=["cur_high","cur_low"]))

df["hourly_vol"]   = df["highVol"].fillna(0) + df["lowVol"].fillna(0)
df["margin"]       = df["cur_high"] - df["cur_low"]
df["net_margin"]   = df["margin"] * 0.98           # 2 % GE tax round-trip
df["roi"]          = df["net_margin"] / df["cur_low"]
df["profit_limit"] = df["net_margin"] * df["limit"].fillna(0)

# ---------- 3) filters -------------------------------------------------------
vol_cut   = df["hourly_vol"] > 500_000
value_cut = df["cur_low"]    > 5_000_000
safe_cut  = (df["avgLowPrice"].notna() &
             (df["cur_low"] - df["avgLowPrice"]
             ).abs() / df["avgLowPrice"] < 0.05)

candidates  = df[safe_cut & (vol_cut | value_cut) & (df["net_margin"] > 0)]

high_value  = (candidates[value_cut]
               .sort_values("net_margin", ascending=False)
               .head(10))

high_volume = (candidates[vol_cut & ~value_cut]
               .sort_values("profit_limit", ascending=False)
               .head(10))

# ---------- 4) display -------------------------------------------------------
print("=== High-value flips ===")
print(high_value[["name","cur_low","cur_high","net_margin"]])

print("\n=== High-volume flips ===")
print(high_volume[["name","hourly_vol","net_margin","profit_limit"]])
