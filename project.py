# import libraries
import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from rapidfuzz import fuzz
from collections import defaultdict, Counter
from datetime import datetime
import math

# Try to import fuzzywuzzy for comparison If not present, skip that part.
try:
    from fuzzywuzzy import fuzz as fw_fuzz
    FUZZYWUZZY_AVAILABLE = True
except Exception:
    FW_WARNING = "fuzzywuzzy not available — comparison with fuzzywuzzy skipped. To enable install `fuzzywuzzy` (`python-levenshtein`)."
    FUZZYWUZZY_AVAILABLE = False

# Page config & styling

st.set_page_config(page_title="MS Excel Deduplicator (85% 90% 95% Thresholds)", layout="wide", initial_sidebar_state="auto")
st.markdown(
    """
    <style>
      body { background-color: #E4A0F7; } /* lavender */
      .title { text-align:center; font-size:34px; font-weight:800; margin-bottom:4px; color:#2e0854; }
      .subtitle { text-align:center; font-size:14px; margin-top:0px; color:#4b0f6f; }
      .developer { position: fixed; right: 14px; bottom: 10px; font-style: italic; color:#2e0854; }
      .stButton>button, .stDownloadButton>button { background-color: #4B0082; color: white; font-weight:700; border-radius:8px; }
      .progress-label { font-weight:700; color:#30204a; }
      table.data { border-collapse: collapse; width: 100%; }
      table.data td, th { border: 1px solid #ddd; padding: 8px; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="title">MS EXCEL DEDUPLICATOR</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Exact & Fuzzy deduplication with 85%, 90%, 95% thresholds runs (RapidFuzz; fuzzywuzzy comparison)</div>', unsafe_allow_html=True)

if not FUZZYWUZZY_AVAILABLE:
    st.warning("Note: fuzzywuzzy not installed — fuzzywuzzy-based comparison rows will be skipped. Install fuzzywuzzy.")


st.markdown("<div class='section'>", unsafe_allow_html=True)

# Helpers

def now_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_action(msg):
    if st.session_state.get("audit_log") is None:
        st.session_state.audit_log = []
    st.session_state.audit_log.insert(0, f"{now_ts()} — {msg}")

def df_to_bytes(df):
    try:
        import openpyxl  # noqa
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="cleaned")
        buf.seek(0)
        return buf.getvalue(), "excel"
    except Exception:
        return df.to_csv(index=False).encode("utf-8"), "csv"

def basic_clean(df):
    df = df.copy()
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).replace({"nan": np.nan, "None": np.nan})
        df[c] = df[c].where(df[c].notnull(), np.nan)
        df[c] = df[c].apply(lambda x: " ".join(x.strip().title().split()) if isinstance(x, str) else x)
    return df

def detect_exact_groups(df, key_cols):
    if not key_cols:
        return []
    grouped = df.groupby(key_cols, dropna=False).indices
    groups = [list(v) for v in grouped.values() if len(v) > 1]
    return groups

def soundex(word: str) -> str:
    if not isinstance(word, str) or word.strip() == "":
        return ""
    w = word.upper()
    mapping = {
        "B":"1","F":"1","P":"1","V":"1",
        "C":"2","G":"2","J":"2","K":"2","Q":"2","S":"2","X":"2","Z":"2",
        "D":"3","T":"3",
        "L":"4",
        "M":"5","N":"5",
        "R":"6"
    }
    first = w[0]
    tail = w[1:]
    prev = mapping.get(first, "")
    digits = []
    for ch in tail:
        code = mapping.get(ch, "0")
        if code != prev:
            digits.append(code)
            prev = code
    digits = [d for d in digits if d != "0"]
    code = first + "".join(digits)
    code = (code + "000")[:4]
    return code

def sorted_token_key(text: str) -> str:
    if not isinstance(text, str):
        return ""
    toks = [t.strip().lower() for t in text.split() if t.strip()]
    toks.sort()
    return " ".join(toks)

# Hybrid blocking pair generation (RapidFuzz scoring)
def fuzzy_pairs_hybrid(df, cols, threshold=85, max_pairs=2_000_000, block_by=None, soundex_cols=None):
    start = time.time()
    n = len(df)
    if n <= 1:
        return [], 0.0
    texts = df[cols].fillna("").astype(str).agg(" | ".join, axis=1).tolist()
    # soundex map
    soundex_map = {}
    if soundex_cols:
        for idx, row in df[soundex_cols].fillna("").astype(str).iterrows():
            parts = [soundex(str(row[c])) if str(row[c]).strip() else "" for c in soundex_cols]
            soundex_map[idx] = "|".join(parts)
    else:
        soundex_map = {i:"" for i in range(n)}
    # sorted keys
    sorted_keys = [sorted_token_key(t) for t in texts]
    blocks = defaultdict(list)
    if soundex_cols:
        for i,k in soundex_map.items():
            if k.strip() == "":
                continue
            blocks[f"sx|{k}"].append(i)
    for i, sk in enumerate(sorted_keys):
        if not sk: continue
        toks = sk.split()
        first = toks[0] if toks else ""
        bucket = abs(hash(sk)) % 10000
        blocks[f"st|{first}|{bucket}"].append(i)
    if block_by:
        for val, idxs in df.groupby(df[block_by].fillna("__MISSING__").astype(str)).indices.items():
            blocks[f"blk|{val}"].extend(list(idxs))
    cleaned_blocks = []
    for k, idxs in blocks.items():
        uniq = list(dict.fromkeys(idxs))
        if len(uniq) > 1:
            cleaned_blocks.append(uniq)
    if not cleaned_blocks:
        cleaned_blocks = [list(range(n))]
    pairs = set()
    total_blocks = len(cleaned_blocks)
    pb = st.progress(0)
    pct_text = st.empty()
    for bi, idxs in enumerate(cleaned_blocks):
        m = len(idxs)
        chunk_size = 2000
        if m > chunk_size:
            chunks = [idxs[i:i+chunk_size] for i in range(0, m, chunk_size)]
        else:
            chunks = [idxs]
        for chunk in chunks:
            L = len(chunk)
            for a in range(L):
                i = chunk[a]
                ti = texts[i]
                for b in range(a+1, L):
                    j = chunk[b]
                    sc = fuzz.token_sort_ratio(ti, texts[j])
                    if sc >= threshold:
                        if i < j:
                            pairs.add((i, j, sc))
                        else:
                            pairs.add((j, i, sc))
                    if len(pairs) >= max_pairs:
                        elapsed = round(time.time() - start, 2)
                        pb.progress(100)
                        pct_text.text("100%")
                        return list(pairs), elapsed
        pb.progress(int((bi+1)/total_blocks*100))
        pct_text.text(f"Detecting... {int((bi+1)/total_blocks*100)}%")
    pb.progress(100); pct_text.text("100%")
    elapsed = round(time.time() - start, 2)
    return list(pairs), elapsed

def cluster_from_pairs_list(pairs):
    parent = {}
    def find(x):
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]
    def union(a,b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra
    for i,j,_ in pairs:
        union(i,j)
    clusters = {}
    for node in parent:
        root = find(node)
        clusters.setdefault(root, []).append(node)
    clusters_list = list(clusters.values())
    clusters_list.sort(key=lambda x: -len(x))
    return clusters_list

# Merge rules: keep details of first farmer, aggregate others
def is_date_like(series):
    try:
        if series.dropna().empty:
            return False
        pd.to_datetime(series.dropna().iloc[0])
        return True
    except Exception:
        return False

def merge_group_keep_first(df, indices, id_cols=None, phone_cols=None, name_cols=None):
    if id_cols is None: id_cols=[]
    if phone_cols is None: phone_cols=[]
    if name_cols is None: name_cols=[]
    indices_sorted = list(indices)
    sub = df.loc[indices_sorted]
    out = {}
    for col in df.columns:
        s = sub[col]
        if col in phone_cols:
            vals = s.dropna().tolist()
            out[col] = vals[0] if vals else np.nan
        elif col in name_cols:
            vals = s.dropna().tolist()
            out[col] = vals[0] if vals else np.nan
        elif pd.api.types.is_numeric_dtype(s):
            if col in id_cols:
                vals = s.dropna().tolist()
                out[col] = vals[0] if vals else np.nan
            else:
                out[col] = s.dropna().astype(float).sum() if not s.dropna().empty else np.nan
        elif pd.api.types.is_datetime64_any_dtype(s) or is_date_like(s):
            vals = pd.to_datetime(s.dropna(), errors='coerce').dropna()
            out[col] = vals.min() if not vals.empty else np.nan
        else:
            vals = s.dropna().tolist()
            out[col] = vals[0] if vals else np.nan
    return out

def merge_groups_with_progress_keep_first(df, groups, id_cols=None, phone_cols=None, name_cols=None):
    start = time.time()
    total = len(groups)
    if total == 0:
        return df.copy(), 0, 0.0
    merged = []
    all_idx = []
    pb = st.progress(0)
    pct_text = st.empty()
    for k, g in enumerate(groups):
        merged.append(merge_group_keep_first(df, g, id_cols=id_cols, phone_cols=phone_cols, name_cols=name_cols))
        all_idx.extend(g)
        pct = int((k+1)/total*100)
        pb.progress(pct)
        pct_text.text(f"Merging... {pct}%")
        time.sleep(0.01)
    df_new = df.drop(index=all_idx).reset_index(drop=True)
    if merged:
        df_new = pd.concat([df_new, pd.DataFrame(merged)], ignore_index=True)
    pb.progress(100); pct_text.text("100%")
    elapsed = round(time.time() - start, 2)
    return df_new, len(all_idx), elapsed

# Session state init

for key in [
    "step","df_raw","df",
    "exact_groups","exact_detect_time","exact_merge_time","exact_removed",
    "fuzzy_runs_results","fuzzy_pairs","fuzzy_clusters","fuzzy_detect_time","fuzzy_merge_time","fuzzy_removed",
    "last_snapshot","audit_log","show_exact_confirm","show_fuzzy_confirm"
]:
    if key not in st.session_state:
        st.session_state[key] = None

if st.session_state.step is None:
    st.session_state.step = 1

# Sidebar
with st.sidebar:
    st.markdown("### 🔍Audit Log")
    if st.session_state.audit_log:
        for a in st.session_state.audit_log[:100]:
            st.write(f"- {a}")
    else:
        st.write("No actions yet.")
    st.write("---")
    if st.button("Restart Wizard"):
        for k in [
            "step","df_raw","df",
            "exact_groups","exact_detect_time","exact_merge_time","exact_removed",
            "fuzzy_runs_results","fuzzy_pairs","fuzzy_clusters","fuzzy_detect_time","fuzzy_merge_time","fuzzy_removed",
            "last_snapshot","audit_log","show_exact_confirm","show_fuzzy_confirm"
        ]:
            st.session_state[k] = None
        st.session_state.step = 1
        st.rerun()
    if st.session_state.last_snapshot is not None:
        if st.button("Undo Last Merge/Delete"):
            st.session_state.df = st.session_state.last_snapshot.copy()
            log_action("Undo last merge/delete")
            st.session_state.last_snapshot = None
            st.rerun()

# Navigation buttons later at bottom
st.markdown("---")

# Step 1: Upload

if st.session_state.step == 1:
    st.markdown("<div class='section'>", unsafe_allow_html=True)
    st.header("Step 1 — Upload dataset (CSV / Excel)")
    uploaded = st.file_uploader("Upload file", type=["csv","xlsx","xls"], key="upload1")
    if uploaded:
        try:
            if uploaded.name.lower().endswith(".csv"):
                st.session_state.df_raw = pd.read_csv(uploaded)
            else:
                st.session_state.df_raw = pd.read_excel(uploaded, engine="openpyxl")
            st.session_state.df = st.session_state.df_raw.copy()
            st.success(f"Loaded {st.session_state.df.shape[0]} rows × {st.session_state.df.shape[1]} columns")
            log_action(f"Uploaded file: {uploaded.name}")
            st.dataframe(st.session_state.df.head(5))
        except Exception as e:
            st.error(f"Failed to load file: {e}")
    st.markdown("</div>", unsafe_allow_html=True)

# Step 2: Summary & Clean
elif st.session_state.step == 2:
    st.markdown("<div class='section'>", unsafe_allow_html=True)
    st.header("Step 2 — Summary & Basic Clean")
    if st.session_state.df is None:
        st.info("Upload file in Step 1 first.")
    else:
        df = st.session_state.df
        st.write(f"Rows: **{df.shape[0]}** | Columns: **{df.shape[1]}**")
        st.write("Top missing counts:")
        st.dataframe(df.isnull().sum().sort_values(ascending=False).to_frame("missing_count"))
        if st.button("Run basic clean (trim & Title Case)"):
            start = time.time()
            with st.spinner("Cleaning..."):
                pb = st.progress(0)
                cleaned = basic_clean(df)
                pb.progress(50)
                time.sleep(0.1)
                st.session_state.df = cleaned
                pb.progress(100)
            elapsed = round(time.time() - start, 2)
            st.session_state.exact_groups = []
            st.session_state.fuzzy_runs_results = None
            log_action(f"Basic clean applied ({elapsed}s)")
            st.success(f"Cleaning finished in {elapsed} seconds.")
            st.dataframe(st.session_state.df.head(5))
    st.markdown("</div>", unsafe_allow_html=True)

# Step 3: Exact detect & optional merge
elif st.session_state.step == 3:
    st.markdown("<div class='section'>", unsafe_allow_html=True)
    st.header("Step 3 — Exact duplicate detection")
    if st.session_state.df is None:
        st.info("Upload & clean first.")
    else:
        df = st.session_state.df
        col_opts = list(df.columns)
        exact_cols = st.multiselect("Select columns to identify exact duplicates", options=col_opts, default=col_opts[:2])
        id_cols_input = st.text_input("Comma-separated ID columns (these will NOT be summed)", value="")
        id_cols = [c.strip() for c in id_cols_input.split(",")] if id_cols_input.strip() else []
        if st.button("Detect exact duplicates"):
            if not exact_cols:
                st.warning("Pick at least one column.")
            else:
                start = time.time()
                with st.spinner("Detecting exact duplicates..."):
                    pb = st.progress(0)
                    groups = detect_exact_groups(df, exact_cols)
                    pb.progress(100)
                elapsed = round(time.time() - start, 2)
                st.session_state.exact_groups = groups
                st.session_state.exact_detect_time = elapsed
                log_action(f"Exact detection finished ({len(groups)} groups in {elapsed}s)")
                st.success(f"Found {len(groups)} exact duplicate groups in {elapsed} seconds.")
                if groups:
                    st.write("Preview first 5 exact groups:")
                    for g in groups[:5]:
                        st.write(df.loc[g])
                    try:
                        dupdf = pd.concat([df.loc[g] for g in groups], ignore_index=True)
                        data_bytes, ftype = df_to_bytes(dupdf)
                        if ftype == "excel":
                            st.download_button("Download Exact Duplicates (Excel)", data=data_bytes, file_name="exact_duplicates.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        else:
                            st.download_button("Download Exact Duplicates (CSV)", data=data_bytes, file_name="exact_duplicates.csv", mime="text/csv")
                    except Exception as e:
                        st.error(f"Prepare download failed: {e}")

        if st.session_state.exact_groups:
            st.markdown("<div class='danger'>", unsafe_allow_html=True)
            st.write("WARNING!!!⚠️You are about to MERGE & DELETE exact duplicates.")
            if st.button("Proceed"):
                st.session_state.show_exact_confirm = True
            if st.button("Cancel"):
                st.session_state.show_exact_confirm = False
            st.markdown("</div>", unsafe_allow_html=True)
            if st.session_state.get("show_exact_confirm", False):
                st.warning("Confirm merge & delete exact duplicates.")
                yes_col, no_col = st.columns([1,1])
                with yes_col:
                    if st.button("Yes"):
                        st.session_state.last_snapshot = st.session_state.df.copy()
                        name_cols = [c for c in df.columns if "name" in c.lower()]
                        phone_candidates = [c for c in df.columns if any(p in c.lower() for p in ["phone","mobile","contact","tel"])]
                        with st.spinner("Merging exact duplicates..."):
                            df_new, removed, elapsed_merge = merge_groups_with_progress_keep_first(st.session_state.df, st.session_state.exact_groups, id_cols=id_cols, phone_cols=phone_candidates, name_cols=name_cols)
                        st.session_state.df = df_new
                        st.session_state.exact_removed = removed
                        st.session_state.exact_merge_time = round(elapsed_merge, 2)
                        st.session_state.exact_groups = []
                        st.session_state.show_exact_confirm = False
                        log_action(f"Exact merged & deleted ({removed} rows in {st.session_state.exact_merge_time}s)")
                        st.success(f"Merged & deleted {removed} exact duplicate rows in {st.session_state.exact_merge_time} seconds.")
                with no_col:
                    if st.button("No — Cancel exact merge"):
                        st.session_state.show_exact_confirm = False
    st.markdown("</div>", unsafe_allow_html=True)

# Step 4: Prepare for fuzzy
elif st.session_state.step == 4:
    st.markdown("<div class='section'>", unsafe_allow_html=True)
    st.header("Step 4 — Prepare for Fuzzy detection")
    if st.session_state.df is None:
        st.info("No dataset available.")
    else:
        st.write(f"Current dataset: {st.session_state.df.shape[0]} rows × {st.session_state.df.shape[1]} cols")
        uploaded_cleaned = st.file_uploader("Optional: upload cleaned file after exact merging", type=["csv","xlsx","xls"], key="upload_after_exact")
        if uploaded_cleaned:
            try:
                if uploaded_cleaned.name.lower().endswith(".csv"):
                    st.session_state.df = pd.read_csv(uploaded_cleaned)
                else:
                    st.session_state.df = pd.read_excel(uploaded_cleaned, engine="openpyxl")
                st.success("Uploaded cleaned file for fuzzy detection.")
                log_action("User uploaded cleaned file for fuzzy detection")
            except Exception as e:
                st.error(f"Failed to load uploaded cleaned file: {e}")
    st.markdown("</div>", unsafe_allow_html=True)

# Step 5: Fuzzy detection across thresholds & merge
elif st.session_state.step == 5:
    st.markdown("<div class='section'>", unsafe_allow_html=True)
    st.header("Step 5 — Fuzzy duplicate detection (85%, 90%, 95%)")
    if st.session_state.df is None:
        st.info("No dataset available.")
    else:
        df = st.session_state.df
        col_opts = list(df.columns)
        fuzzy_cols = st.multiselect("Select columns for fuzzy comparison", options=col_opts, default=col_opts[:2])
        block_by = st.selectbox("Optional block-by column (reduces comparisons)", options=[None] + col_opts, index=0)
        soundex_cols = st.multiselect("Soundex columns (helpful for names/phones)", options=col_opts, default=[c for c in col_opts if "name" in c.lower()][:1])
        max_pairs = st.number_input("Max candidate pairs safeguard", min_value=1000, max_value=50_000_000, value=2_000_000, step=1000)

        thresholds = [85, 90, 95]
        if st.button("Run fuzzy detection for 85%, 90%, 95% sequentially"):
            if not fuzzy_cols:
                st.warning("Select columns to compare.")
            else:
                results = []
                for thr in thresholds:
                    st.write(f"---\n### Running threshold {thr}%")
                    with st.spinner(f"Detecting pairs at {thr}%..."):
                        pairs, elapsed = fuzzy_pairs_hybrid(df, fuzzy_cols, threshold=thr, max_pairs=max_pairs, block_by=(block_by if block_by else None), soundex_cols=(soundex_cols if soundex_cols else None))
                        clusters = cluster_from_pairs_list(pairs)
                        count_pairs = len(pairs)
                        count_clusters = len(clusters)
                    # sample up to 200 pairs to compute avg rapidfuzz score and fuzzywuzzy if available
                    sample_n = min(200, count_pairs)
                    avg_rscore = None
                    avg_fwscore = None
                    if sample_n > 0:
                        sample_pairs = pairs[:sample_n]
                        rsum = 0.0
                        fwsum = 0.0
                        for (i,j,sc) in sample_pairs:
                            rsum += sc
                            if FUZZYWUZZY_AVAILABLE:
                                try:
                                    fwsum += fw_fuzz.token_sort_ratio(str(df.loc[i, fuzzy_cols].fillna("").astype(str).agg(" | ".join)), str(df.loc[j, fuzzy_cols].fillna("").astype(str).agg(" | ".join)))
                                except Exception:
                                    fwsum += 0.0
                        avg_rscore = rsum / sample_n
                        if FUZZYWUZZY_AVAILABLE:
                            avg_fwscore = fwsum / sample_n
                    else:
                        avg_rscore = 0.0
                        avg_fwscore = 0.0 if FUZZYWUZZY_AVAILABLE else None

                    results.append({
                        "threshold": thr,
                        "pairs": count_pairs,
                        "clusters": count_clusters,
                        "time_s": elapsed,
                        "avg_rapidfuzz_sample_score": round(float(avg_rscore), 2) if avg_rscore is not None else None,
                        "avg_fuzzywuzzy_sample_score": round(float(avg_fwscore), 2) if (avg_fwscore is not None and FUZZYWUZZY_AVAILABLE) else (None if FUZZYWUZZY_AVAILABLE else "N/A")
                    })
                    st.write(f"Threshold {thr}% -> pairs: {count_pairs}, clusters: {count_clusters}, time: {elapsed} s")
                    if sample_n > 0:
                        st.write(f"Sample avg RapidFuzz score: {round(avg_rscore,2)}" + (f"; sample avg FuzzyWuzzy score: {round(avg_fwscore,2)}" if FUZZYWUZZY_AVAILABLE and avg_fwscore is not None else ""))
                    else:
                        st.write("No candidate pairs at this threshold.")
                    # save results to state as we go
                    st.session_state.fuzzy_runs_results = results
                # after loop, show comparison table
                st.write("## Threshold comparison summary")
                df_res = pd.DataFrame(results)
                if not df_res.empty:
                    df_res["time_min"] = (df_res["time_s"] / 60).round(2)
                    st.dataframe(df_res[["threshold","pairs","clusters","time_s","time_min","avg_rapidfuzz_sample_score","avg_fuzzywuzzy_sample_score"]])
                    # recommendation heuristic:
                    # quality ~ avg_rapidfuzz_sample_score (higher is better)
                    # speed penalty ~ time_min (lower is better)
                    # coverage ~ clusters (higher is better)
                    # define score = (avg_score_norm * log(1+clusters)) / (1 + time_min)
                    rec_rows = []
                    for _, row in df_res.iterrows():
                        avg_score = row["avg_rapidfuzz_sample_score"] if row["avg_rapidfuzz_sample_score"] is not None else 0.0
                        clusters = max(1, row["clusters"])
                        time_min = max(0.001, row["time_s"]/60.0)
                        avg_norm = avg_score / 100.0
                        metric = (avg_norm * math.log(1+clusters)) / (1 + time_min)
                        rec_rows.append((row["threshold"], metric))
                    # recommend highest metric
                    rec_rows.sort(key=lambda x: -x[1])
                    recommended_threshold = rec_rows[0][0] if rec_rows else thresholds[0]
                    st.success(f"Recommended threshold (heuristic balancing speed & quality): **{recommended_threshold}%**")
                    st.write("Recommendation logic: balances sample similarity, number of clusters found (proxy for coverage), and runtime (minutes). This is a heuristic; review previewed pairs to confirm.")
                    log_action(f"Fuzzy thresholds compared; recommended {recommended_threshold}%")
                else:
                    st.info("No results to summarize.")
        # allow download of last run's duplicate sets if available
        if st.session_state.fuzzy_runs_results:
            st.write("You can download candidate pairs from the last-run threshold results (if you want to inspect).")
            last = st.session_state.fuzzy_runs_results[-1]
            thr = last["threshold"]
            st.write(f"Last run threshold: {thr}% -> pairs: {last['pairs']} clusters: {last['clusters']}")
            # We did not store pairs per threshold to avoid massive memory — re-run specific threshold to export if desired
            if st.button("Re-run selected threshold to export duplicates (choose threshold)"):
                thr_choice = st.selectbox("Choose threshold to re-run for export", options=[r["threshold"] for r in st.session_state.fuzzy_runs_results], index=0)
                with st.spinner(f"Re-running threshold {thr_choice} for export..."):
                    pairs, elapsed = fuzzy_pairs_hybrid(st.session_state.df, fuzzy_cols, threshold=thr_choice, max_pairs=max_pairs, block_by=(block_by if block_by else None), soundex_cols=(soundex_cols if soundex_cols else None))
                    clusters = cluster_from_pairs_list(pairs)
                    df = st.session_state.df
                    if clusters:
                        dup_frames = [df.loc[g] for g in clusters]
                        all_dups = pd.concat(dup_frames, ignore_index=True)
                        data_bytes, ftype = df_to_bytes(all_dups)
                        if ftype == "excel":
                            st.download_button(f"Download duplicates @ {thr_choice}% (Excel)", data=data_bytes, file_name=f"fuzzy_duplicates_{thr_choice}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        else:
                            st.download_button(f"Download duplicates @ {thr_choice}% (CSV)", data=data_bytes, file_name=f"fuzzy_duplicates_{thr_choice}.csv", mime="text/csv")
                    else:
                        st.info("No duplicates at this threshold.")
        # Provide option to merge using recommended threshold
        if st.session_state.fuzzy_runs_results:
            if st.button("Merge using recommended threshold from last comparison"):
                # compute recommended threshold again from stored results
                df_res = pd.DataFrame(st.session_state.fuzzy_runs_results)
                rec_rows = []
                for _, row in df_res.iterrows():
                    avg_score = row["avg_rapidfuzz_sample_score"] if row["avg_rapidfuzz_sample_score"] is not None else 0.0
                    clusters = max(1, row["clusters"])
                    time_min = max(0.001, row["time_s"]/60.0)
                    avg_norm = avg_score / 100.0
                    metric = (avg_norm * math.log(1+clusters)) / (1 + time_min)
                    rec_rows.append((row["threshold"], metric))
                rec_rows.sort(key=lambda x: -x[1])
                recommended_threshold = rec_rows[0][0] if rec_rows else 90
                # re-run that threshold to get clusters then merge
                st.session_state.last_snapshot = st.session_state.df.copy()
                with st.spinner(f"Re-running threshold {recommended_threshold}% and merging..."):
                    pairs, elapsed = fuzzy_pairs_hybrid(st.session_state.df, fuzzy_cols, threshold=recommended_threshold, max_pairs=max_pairs, block_by=(block_by if block_by else None), soundex_cols=(soundex_cols if soundex_cols else None))
                    clusters = cluster_from_pairs_list(pairs)
                    if not clusters:
                        st.info("No fuzzy clusters found at recommended threshold; nothing to merge.")
                    else:
                        name_cols = [c for c in df.columns if "name" in c.lower()]
                        phone_candidates = [c for c in df.columns if any(p in c.lower() for p in ["phone","mobile","contact","tel"])]
                        df_new, removed, elapsed_merge = merge_groups_with_progress_keep_first(st.session_state.df, clusters, id_cols=None, phone_cols=phone_candidates, name_cols=name_cols)
                        st.session_state.df = df_new
                        st.session_state.fuzzy_removed = removed
                        st.session_state.fuzzy_merge_time = round(elapsed_merge, 2)
                        st.session_state.fuzzy_clusters = []
                        log_action(f"Merged fuzzy duplicates at {recommended_threshold}% -> removed {removed} rows in {st.session_state.fuzzy_merge_time}s")
                        st.success(f"Merged & deleted {removed} fuzzy duplicates at {recommended_threshold}% in {st.session_state.fuzzy_merge_time} seconds.")
    st.markdown("</div>", unsafe_allow_html=True)

# Step 6: Final download & summary
elif st.session_state.step == 6:
    st.markdown("<div class='section'>", unsafe_allow_html=True)
    st.header("Step 6 — Final cleaned dataset & summary")
    if st.session_state.df is None:
        st.info("No dataset available.")
    else:
        df = st.session_state.df
        bytes_out, ftype = df_to_bytes(df)
        if ftype == "excel":
            st.download_button("Download Final Cleaned Excel", data=bytes_out, file_name="cleaned_dataset.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.download_button("Download Final Cleaned CSV", data=bytes_out, file_name="cleaned_dataset.csv", mime="text/csv")
        st.markdown("### Summary of actions")
        st.write(f"- Exact detection time: **{st.session_state.exact_detect_time if st.session_state.exact_detect_time else 0}** s")
        st.write(f"- Exact rows removed (merged): **{st.session_state.exact_removed if st.session_state.exact_removed else 0}**")
        st.write(f"- Exact merge time: **{st.session_state.exact_merge_time if st.session_state.exact_merge_time else 0}** s")
        if st.session_state.fuzzy_runs_results:
            res_df = pd.DataFrame(st.session_state.fuzzy_runs_results)
            st.write("### Fuzzy runs (last comparison)")
            st.dataframe(res_df)
        st.write(f"- Fuzzy rows removed (merged): **{st.session_state.fuzzy_removed if st.session_state.fuzzy_removed else 0}**")
        st.write(f"- Final rows: **{df.shape[0]}**")
        st.success("Process complete — use Undo in sidebar to revert the most recent merge if needed.")
    st.markdown("</div>", unsafe_allow_html=True)

# Bottom navigation
st.markdown("<div class='footer-space'></div>", unsafe_allow_html=True)
nav_left, nav_right = st.columns([1,1])
with nav_left:
    if st.button("◀ Back"):
        st.session_state.step = max(1, st.session_state.step - 1)
        st.rerun()
with nav_right:
    if st.button("Next ▶"):
        st.session_state.step = min(6, st.session_state.step + 1)
        st.rerun()

# Developer credit
st.markdown('<div class="developer">(Developer: Tobias Abor)</div>', unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)
