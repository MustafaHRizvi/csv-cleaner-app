import os, re, io, gc, json, tempfile
import requests
import pandas as pd
import tldextract
import streamlit as st
from zipfile import ZipFile
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

CHUNK_SIZE      = 50_000
TCPA_BASE_URL   = "https://api.tcpalitigatorlist.com"
TCPA_BACKUP_URL = "https://api101.tcpalitigatorlist.com"
TCPA_BATCH_SIZE = 3_000
TCPA_USER       = os.environ.get("TCPA_USER", "")
TCPA_PASS       = os.environ.get("TCPA_PASS", "")
ADMIN_PASSWORD  = os.environ.get("ADMIN_PASSWORD", "")

_SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
INTERNAL_DNC_DIR = os.environ.get("INTERNAL_DNC_DIR", os.path.join(_SCRIPT_DIR, "internal_dnc"))

# Google Drive file IDs for the three internal DNC files
DRIVE_FILE_IDS = {
    "Emails.csv":  os.environ.get("EMAILS_DRIVE_ID", ""),
    "Phones.csv":  os.environ.get("PHONES_DRIVE_ID", ""),
    "Domains.csv": os.environ.get("DOMAINS_DRIVE_ID", ""),
}

# tcpa_dnc_status labels — match HubSpot property values
LABEL_INTERNAL_DNC = "internal_dnc"   # used for both internal DNC and uploaded suppression files


# ============================================================
# SAVE UPLOADED FILE TO DISK
# ============================================================
def save_uploaded_to_disk(uploaded_file):
    suffix = os.path.splitext(uploaded_file.name)[1] or ".csv"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(uploaded_file.getbuffer())
    tmp_path = tmp.name
    tmp.close()
    return tmp_path


# ============================================================
# COLUMN FINDER
# ============================================================
def find_col(df, patterns):
    for col in df.columns:
        lc = col.lower()
        for p in patterns:
            if p in lc:
                return col
    return None


# ============================================================
# CLEANING HELPERS
# ============================================================
def clean_email(email):
    if pd.isna(email): return None
    e = str(email).strip().lower()
    return re.sub(r"\s+", "", e)

def clean_phone(phone):
    if pd.isna(phone): return None
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if digits else None

def clean_domain(value):
    if pd.isna(value): return None
    ext = tldextract.extract(str(value).strip().lower())
    if not ext.domain: return None
    return f"{ext.domain}.{ext.suffix}"

def normalize_suppression_email(e):
    if pd.isna(e): return None
    e = str(e).strip().lower()
    e = re.sub(r"[\"'\s]", "", e)
    e = re.sub(r"^email[:\-]*", "", e)
    return e


# ============================================================
# LOAD INTERNAL DNC (server-side folder)
# ============================================================
def load_internal_dnc():
    emails, phones, domains = set(), set(), set()
    logs = []

    mapping = {
        "Emails.csv":  ("Email",  "email",  normalize_suppression_email, emails),
        "Phones.csv":  ("Phone",  "phone",  clean_phone,                 phones),
        "Domains.csv": ("Domain", "domain", clean_domain,                domains),
    }

    if not os.path.isdir(INTERNAL_DNC_DIR):
        logs.append(f"⚠️ Internal DNC folder not found at: {INTERNAL_DNC_DIR}")
        return {"emails": emails, "phones": phones, "domains": domains, "logs": logs}

    for filename, (preferred_col, fallback_hint, normalise, target_set) in mapping.items():
        path = os.path.join(INTERNAL_DNC_DIR, filename)
        if not os.path.exists(path):
            logs.append(f"⚠️ {filename} not found — skipping")
            continue
        try:
            df = pd.read_csv(path, dtype=str)
            col = preferred_col if preferred_col in df.columns else next(
                (c for c in df.columns if fallback_hint in c.lower()), None
            )
            if col is None:
                logs.append(f"⚠️ {filename}: no usable column found")
                continue
            values = df[col].dropna().map(normalise).dropna()
            target_set.update(values)
            logs.append(f"✅ Internal DNC / {filename}: {len(target_set):,} entries loaded")
        except Exception as exc:
            logs.append(f"⚠️ {filename} failed to load: {exc}")

    emails.discard(None)
    phones.discard(None)
    domains.discard(None)
    return {"emails": emails, "phones": phones, "domains": domains, "logs": logs}


# ============================================================
# REFRESH INTERNAL DNC FROM GOOGLE DRIVE
# ============================================================
def _count_rows(path):
    """Return data row count (excluding header) or None if file doesn't exist."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            return sum(1 for _ in f) - 1  # subtract header
    except Exception:
        return None


def refresh_internal_dnc_from_drive():
    missing = [name for name, fid in DRIVE_FILE_IDS.items() if not fid]
    if missing:
        return False, [], f"Drive file IDs not configured for: {', '.join(missing)}"

    os.makedirs(INTERNAL_DNC_DIR, exist_ok=True)
    results = []  # list of dicts for the summary table
    error_msg = None

    for filename, file_id in DRIVE_FILE_IDS.items():
        url  = f"https://drive.google.com/uc?export=download&id={file_id}"
        dest = os.path.join(INTERNAL_DNC_DIR, filename)
        rows_before = _count_rows(dest)

        try:
            resp = requests.get(url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
            if b"virus scan warning" in resp.content[:2000].lower():
                confirm_url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
                resp = requests.get(confirm_url, timeout=60, allow_redirects=True)
                resp.raise_for_status()
            with open(dest, "wb") as f:
                f.write(resp.content)
            rows_after = _count_rows(dest)
            results.append({
                "File":         filename,
                "Rows Before":  rows_before if rows_before is not None else "—",
                "Rows After":   rows_after  if rows_after  is not None else "—",
                "Status":       "✅ Updated",
            })
        except Exception as exc:
            results.append({
                "File":        filename,
                "Rows Before": rows_before if rows_before is not None else "—",
                "Rows After":  "—",
                "Status":      f"⚠️ Failed: {exc}",
            })
            error_msg = str(exc)

    ok = error_msg is None
    return ok, results, error_msg


# ============================================================
# LOAD SUPPRESSION DATA FROM UPLOADED FILES
# ============================================================
def load_suppression_data(files):
    emails, phones, domains = set(), set(), set()
    logs = []

    for f in files:
        fname = getattr(f, "name", str(f))
        try:
            # Read in chunks — suppression files can be very large (700K+ rows)
            found = []
            col_types = {}   # col -> "email" | "phone" | "domain"

            for chunk in pd.read_csv(
                f, dtype=str, chunksize=50_000,
                engine="python", on_bad_lines="skip",
            ):
                if not col_types:
                    for c in chunk.columns:
                        lc = c.lower()
                        if "email" in lc:
                            col_types[c] = "email"
                            found.append(c)
                        elif "phone" in lc:
                            col_types[c] = "phone"
                            found.append(c)
                        elif any(x in lc for x in ["domain", "website", "url"]):
                            col_types[c] = "domain"
                            found.append(c)

                for c, kind in col_types.items():
                    if kind == "email":
                        emails.update(chunk[c].dropna().map(normalize_suppression_email))
                    elif kind == "phone":
                        phones.update(chunk[c].dropna().map(clean_phone))
                    elif kind == "domain":
                        domains.update(chunk[c].dropna().map(clean_domain))

            logs.append(
                f"✅ {fname}: loaded {', '.join(found) if found else 'no usable columns'} "
                f"({len(emails):,} emails / {len(phones):,} phones / {len(domains):,} domains so far)"
            )
        except Exception as e:
            logs.append(f"⚠️ {fname} skipped: {e}")

    emails.discard(None)
    phones.discard(None)
    domains.discard(None)
    return {"emails": emails, "phones": phones, "domains": domains, "logs": logs}


# ============================================================
# CLEAN ONE CHUNK — returns (clean_df, dnc_df)
# dnc_df has tcpa_dnc_status column set to label
# ============================================================
def clean_chunk(df, suppression, label):
    mask_keep = pd.Series(True, index=df.index)

    # ---- Email ----
    strict_email_col = find_col(df, ["email"])
    fallback_email_cols = [c for c in df.columns if "email" in c.lower()]
    email_cols = []
    if strict_email_col:
        email_cols.append(strict_email_col)
    for c in fallback_email_cols:
        if c not in email_cols:
            email_cols.append(c)

    for col in email_cols:
        cleaned = df[col].map(clean_email)
        mask_keep &= ~cleaned.isin(suppression["emails"])

    # ---- Phone ----
    for col in [c for c in df.columns if "phone" in c.lower()]:
        cleaned = df[col].map(clean_phone)
        mask_keep &= ~cleaned.isin(suppression["phones"])

    # ---- Domain ----
    for col in [c for c in df.columns if any(x in c.lower() for x in ["domain", "website", "url"])]:
        cleaned = df[col].map(clean_domain)
        mask_keep &= ~cleaned.isin(suppression["domains"])

    clean_df = df[mask_keep].copy()
    dnc_df   = df[~mask_keep].copy()
    if not dnc_df.empty:
        dnc_df["tcpa_dnc_status"] = label

    removed = (~mask_keep).sum()
    return clean_df, dnc_df, removed


# ============================================================
# MEMORY-SAFE THREE-PASS PROCESSOR
# ============================================================
def process_files(files_to_clean, internal_dnc, extra_suppression):
    summary, logs = [], []
    clean_paths         = {}   # name -> temp path for CLEAR file
    tcpa_internal_paths = {}   # name -> temp path for TCPA+Internal file
    dnc_paths           = {}   # name -> temp path for DNC file (federal/state/complainer)

    has_extra = bool(
        extra_suppression["emails"] or
        extra_suppression["phones"] or
        extra_suppression["domains"]
    )

    total_files   = len(files_to_clean)
    global_bar    = st.progress(0)
    global_status = st.empty()

    for file_index, uploaded in enumerate(files_to_clean, start=1):
        global_status.write(f"Processing {uploaded.name} ({file_index}/{total_files})")
        global_bar.progress(int((file_index - 1) / total_files * 100))

        source_path = save_uploaded_to_disk(uploaded)

        clean_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        clean_path = clean_tmp.name; clean_tmp.close()

        ti_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        ti_path = ti_tmp.name; ti_tmp.close()

        dnc_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        dnc_path = dnc_tmp.name; dnc_tmp.close()

        clean_first = ti_first = True
        rows_before = 0
        cols_found  = []
        rem_idnc = rem_sup = 0

        file_bar    = st.progress(0)
        file_status = st.empty()
        chunk_counter = 0

        try:
            for chunk in pd.read_csv(
                source_path, dtype=str, chunksize=CHUNK_SIZE,
                sep=",", engine="python", on_bad_lines="skip"
            ):
                chunk_counter += 1
                rows_before += len(chunk)

                for c in chunk.columns:
                    lc = c.lower()
                    if "email" in lc: cols_found.append(c)
                    if "phone" in lc: cols_found.append(c)
                    if any(x in lc for x in ["domain", "website", "url"]): cols_found.append(c)

                # Pass 1 — Internal DNC → TCPA+Internal file
                chunk, dnc1, r1 = clean_chunk(chunk, internal_dnc, LABEL_INTERNAL_DNC)
                rem_idnc += r1
                if not dnc1.empty:
                    dnc1.to_csv(ti_path, index=False, mode="a", header=ti_first)
                    ti_first = False

                # Pass 2 — uploaded suppression files → TCPA+Internal file
                if has_extra:
                    chunk, dnc2, r2 = clean_chunk(chunk, extra_suppression, LABEL_INTERNAL_DNC)
                    rem_sup += r2
                    if not dnc2.empty:
                        dnc2.to_csv(ti_path, index=False, mode="a", header=ti_first)
                        ti_first = False

                chunk.to_csv(clean_path, index=False, mode="a", header=clean_first)
                clean_first = False

                file_bar.progress(min(100, chunk_counter * 5))
                file_status.write(f"{uploaded.name}: processed {chunk_counter} chunks…")

                del chunk
                gc.collect()

            total_removed = rem_idnc + rem_sup
            rows_after    = rows_before - total_removed
            logs.append(f"✔ {uploaded.name}: {rem_idnc:,} internal DNC, {rem_sup:,} suppression list")

            summary.append({
                "File":                      uploaded.name,
                "Identified Columns":        ", ".join(sorted(set(cols_found))) or "None",
                "Rows Before":               rows_before,
                "Removed by Internal DNC":   rem_idnc,
                "Removed by Suppression":    rem_sup,
                "Removed by TCPA Litigator": 0,
                "Removed by DNC (API)":      0,
                "Total Removed":             total_removed,
                "Rows After":                rows_after,
            })
            clean_paths[uploaded.name]         = clean_path
            tcpa_internal_paths[uploaded.name] = ti_path
            dnc_paths[uploaded.name]           = dnc_path

        except Exception as e:
            logs.append(f"⚠️ {uploaded.name} failed: {e}")

        finally:
            try: os.remove(source_path)
            except: pass

    global_bar.progress(100)
    global_status.write("Cleaning complete.")
    return pd.DataFrame(summary), logs, clean_paths, tcpa_internal_paths, dnc_paths


# ============================================================
# TCPA API HELPERS
# ============================================================
def _tcpa_post(phones_batch, user, password, base_url):
    resp = requests.post(
        f"{base_url}/scrub/phones/",
        auth=(user, password),
        data={"phones": json.dumps(phones_batch), "type": "all", "small_list": True},
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return {item["phone_number"]: item for item in data}
    if isinstance(data, dict) and "results" in data:
        return {item["phone_number"]: item for item in data["results"]}
    return {}

def scrub_tcpa_batch(phones_batch, user, password):
    try:
        return _tcpa_post(phones_batch, user, password, TCPA_BASE_URL)
    except Exception:
        return _tcpa_post(phones_batch, user, password, TCPA_BACKUP_URL)

def collect_phones_from_files(clean_paths):
    all_phones: set[str] = set()
    for path in clean_paths.values():
        try:
            for chunk in pd.read_csv(path, dtype=str, chunksize=CHUNK_SIZE,
                                     engine="python", on_bad_lines="skip"):
                for col in [c for c in chunk.columns if "phone" in c.lower()]:
                    all_phones.update(chunk[col].dropna().map(clean_phone).dropna())
        except Exception:
            pass
    all_phones.discard(None)
    all_phones.discard("")
    return all_phones


def tcpa_scrub_cleaned_files(clean_paths, tcpa_internal_paths, dnc_paths, summary_df, user, password):
    st.info("Collecting phone numbers from cleaned files…")
    all_phones = collect_phones_from_files(clean_paths)
    phone_list = list(all_phones)
    st.write(f"  {len(phone_list):,} unique phone numbers to scrub")

    # phone -> tcpa_dnc_status label derived from API response
    phone_status: dict[str, str] = {}
    total_batches = max(1, (len(phone_list) + TCPA_BATCH_SIZE - 1) // TCPA_BATCH_SIZE)
    tcpa_bar    = st.progress(0)
    tcpa_status = st.empty()

    for i, start in enumerate(range(0, len(phone_list), TCPA_BATCH_SIZE), 1):
        batch = phone_list[start : start + TCPA_BATCH_SIZE]
        tcpa_status.write(f"TCPA scrub: batch {i}/{total_batches} ({len(batch)} numbers)…")
        try:
            results = scrub_tcpa_batch(batch, user, password)
            for phone, result in results.items():
                if str(result.get("clean", "1")) == "0":
                    status_array = result.get("status_array") or []
                    if status_array:
                        mapping = {
                            "federal_dnc":  "federal_dnc",
                            "state_dnc":    "state_dnc",
                            "tcpa":         "tcpa_litigator",
                            "complainers":  "complainer",
                            "complainer":   "complainer",
                        }
                        vals = [mapping[v] for v in status_array if v in mapping]
                    else:
                        vals = []
                        if result.get("on_federal_dnc") == "Y": vals.append("federal_dnc")
                        if result.get("on_state_dnc")   == "Y": vals.append("state_dnc")
                        if result.get("on_tcpa")         == "Y": vals.append("tcpa_litigator")
                        if result.get("on_complainers")  == "Y": vals.append("complainer")
                    phone_status[phone] = ";".join(vals) if vals else "federal_dnc"
        except Exception as exc:
            st.warning(f"Batch {i} failed: {exc}")
        tcpa_bar.progress(int(i / total_batches * 100))

    tcpa_bar.progress(100)
    flagged_phones = set(phone_status.keys())
    st.write(f"  {len(flagged_phones):,} phone numbers flagged by TCPA")

    if not flagged_phones:
        st.write("  No TCPA-flagged numbers — no rows removed.")
        return summary_df

    for name in list(clean_paths.keys()):
        clean_path = clean_paths[name]
        ti_path    = tcpa_internal_paths[name]
        dnc_path   = dnc_paths[name]

        new_clean_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        new_clean_path = new_clean_tmp.name; new_clean_tmp.close()

        clean_first  = True
        ti_has_rows  = os.path.exists(ti_path)  and os.path.getsize(ti_path)  > 0
        dnc_has_rows = os.path.exists(dnc_path) and os.path.getsize(dnc_path) > 0
        rem_litigator = rem_dnc_api = 0

        try:
            for chunk in pd.read_csv(clean_path, dtype=str, chunksize=CHUNK_SIZE,
                                     engine="python", on_bad_lines="skip"):
                phone_cols = [c for c in chunk.columns if "phone" in c.lower()]
                mask_flag     = pd.Series(False, index=chunk.index)
                matched_phone = pd.Series("",    index=chunk.index)
                for col in phone_cols:
                    cleaned = chunk[col].map(clean_phone)
                    hits = cleaned.isin(flagged_phones)
                    mask_flag    |= hits
                    matched_phone = matched_phone.where(
                        matched_phone != "", cleaned.where(hits, "")
                    )

                flagged_chunk = chunk[mask_flag].copy()
                if not flagged_chunk.empty:
                    flagged_chunk["tcpa_dnc_status"] = (
                        matched_phone[mask_flag].map(
                            lambda p: phone_status.get(p, "federal_dnc")
                        )
                    )
                    # Split: tcpa_litigator → TCPA+Internal; everything else → DNC
                    mask_litigator = flagged_chunk["tcpa_dnc_status"].str.contains(
                        "tcpa_litigator", na=False
                    )
                    ti_chunk  = flagged_chunk[mask_litigator]
                    dnc_chunk = flagged_chunk[~mask_litigator]

                    if not ti_chunk.empty:
                        ti_chunk.to_csv(ti_path, index=False, mode="a", header=not ti_has_rows)
                        ti_has_rows = True
                        rem_litigator += len(ti_chunk)

                    if not dnc_chunk.empty:
                        dnc_chunk.to_csv(dnc_path, index=False, mode="a", header=not dnc_has_rows)
                        dnc_has_rows = True
                        rem_dnc_api += len(dnc_chunk)

                clean_chunk_out = chunk[~mask_flag]
                clean_chunk_out.to_csv(new_clean_path, index=False, mode="a", header=clean_first)
                clean_first = False

                del chunk
                gc.collect()

            os.remove(clean_path)
            clean_paths[name] = new_clean_path

            tcpa_removed = rem_litigator + rem_dnc_api
            mask = summary_df["File"] == name
            summary_df.loc[mask, "Removed by TCPA Litigator"] = rem_litigator
            summary_df.loc[mask, "Removed by DNC (API)"]      = rem_dnc_api
            summary_df.loc[mask, "Total Removed"]             += tcpa_removed
            summary_df.loc[mask, "Rows After"]                -= tcpa_removed

            st.write(
                f"  ✔ {name}: {rem_litigator:,} TCPA litigator, "
                f"{rem_dnc_api:,} DNC (federal/state/complainer)"
            )

        except Exception as exc:
            st.warning(f"  ⚠️ {name} TCPA pass failed: {exc}")
            try: os.remove(new_clean_path)
            except: pass

    return summary_df


# ============================================================
# STREAMLIT UI
# ============================================================
st.set_page_config(page_title="CSV Cleaner", layout="wide")
st.title("🧹 CSV Cleaner")

st.subheader("1️⃣ Upload Files to Clean")
clean_files = st.file_uploader(
    "Upload CSV files to clean", type="csv", accept_multiple_files=True
)

st.subheader("2️⃣ Additional Suppression Files (Optional)")
st.caption("Internal DNC list is applied automatically. Upload additional suppression files here if needed.")
sup_files = st.file_uploader(
    "Upload suppression CSV files", type="csv", accept_multiple_files=True
)

st.subheader("3️⃣ TCPA Phone Scrub")
enable_tcpa = st.checkbox("Scrub phone numbers against TCPA Litigator List", value=True)

if st.button("Run Cleaning"):
    if not clean_files:
        st.error("Please upload at least one file to clean.")
    elif enable_tcpa and not (TCPA_USER and TCPA_PASS):
        st.error("TCPA credentials are not configured on the server. Contact the administrator.")
    else:
        start = datetime.now()

        st.info("Loading Internal DNC list…")
        internal_dnc = load_internal_dnc()
        for log in internal_dnc["logs"]:
            st.write(log)

        extra_suppression = {"emails": set(), "phones": set(), "domains": set(), "logs": []}
        if sup_files:
            st.info("Loading additional suppression files…")
            extra_suppression = load_suppression_data(sup_files)
            for log in extra_suppression["logs"]:
                st.write(log)

        st.info("Running suppression passes…")
        summary_df, logs, clean_paths, tcpa_internal_paths, dnc_paths = process_files(
            clean_files, internal_dnc, extra_suppression
        )
        for log in logs:
            st.write(log)

        if enable_tcpa:
            st.info("Running TCPA phone scrub…")
            summary_df = tcpa_scrub_cleaned_files(
                clean_paths, tcpa_internal_paths, dnc_paths, summary_df, TCPA_USER, TCPA_PASS
            )

        st.info("Preparing ZIP…")
        zip_buffer = io.BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            for name, path in clean_paths.items():
                zf.write(path, arcname=f"CLEAR_{name}")
            for name, path in tcpa_internal_paths.items():
                if os.path.exists(path) and os.path.getsize(path) > 0:
                    zf.write(path, arcname=f"TCPA+Internal_{name}")
            for name, path in dnc_paths.items():
                if os.path.exists(path) and os.path.getsize(path) > 0:
                    zf.write(path, arcname=f"DNC_{name}")
            zf.writestr("_Cleaning_Summary.csv", summary_df.to_csv(index=False))
        zip_buffer.seek(0)

        for p in list(clean_paths.values()) + list(tcpa_internal_paths.values()) + list(dnc_paths.values()):
            try: os.remove(p)
            except: pass

        st.subheader("📊 Summary")
        st.dataframe(summary_df)

        st.download_button(
            "⬇️ Download Cleaned Files (ZIP)",
            data=zip_buffer,
            file_name="Cleaned_Files.zip",
            mime="application/zip"
        )

        st.success(f"✨ Done! Total time: {datetime.now() - start}")

# ── Admin panel ───────────────────────────────────────────────────────────────
st.divider()
with st.expander("🔧 Admin"):
    if not ADMIN_PASSWORD:
        st.warning("ADMIN_PASSWORD env var is not set — admin panel is disabled.")
    else:
        pwd = st.text_input("Admin password", type="password", key="admin_pwd")
        admin_ok = pwd == ADMIN_PASSWORD

        # ── Refresh DNC files ────────────────────────────────────────────────
        st.markdown("#### Refresh Internal DNC Files")
        if st.button("Refresh from Google Drive"):
            if not admin_ok:
                st.error("Incorrect password.")
            else:
                with st.spinner("Downloading files from Google Drive…"):
                    ok, results, _ = refresh_internal_dnc_from_drive()
                if results:
                    st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
                if ok:
                    st.success("Internal DNC files updated successfully.")
                else:
                    st.error("One or more files failed to download — see table above.")

        st.divider()

        # ── Internal DNC lookup ──────────────────────────────────────────────
        st.markdown("#### Look Up a Contact in Internal DNC Lists")
        lookup_val = st.text_input(
            "Email, phone number, or domain",
            placeholder="e.g. john@example.com  or  7326917161  or  example.com",
            key="lookup_val",
        )
        if st.button("Check Internal DNC", key="btn_lookup"):
            if not admin_ok:
                st.error("Incorrect password.")
            elif not lookup_val.strip():
                st.warning("Enter a value to look up.")
            else:
                dnc = load_internal_dnc()
                v = lookup_val.strip()
                found = []

                # Email exact match
                cleaned_email = clean_email(v)
                if cleaned_email and cleaned_email in dnc["emails"]:
                    found.append("✅ **Email** — matched in Emails.csv")

                # Domain match (either bare domain or extracted from email)
                cleaned_domain = clean_domain(v)
                if cleaned_domain and cleaned_domain in dnc["domains"]:
                    found.append(f"✅ **Domain** (`{cleaned_domain}`) — matched in Domains.csv")

                # Phone match
                cleaned_phone = clean_phone(v)
                if cleaned_phone and cleaned_phone in dnc["phones"]:
                    found.append(f"✅ **Phone** (`{cleaned_phone}`) — matched in Phones.csv")

                if found:
                    for line in found:
                        st.markdown(line)
                else:
                    st.info("Not found in any internal DNC list.")

        st.divider()

        # ── Single TCPA lookup ───────────────────────────────────────────────
        st.markdown("#### Check a Phone Number Against TCPA API")
        tcpa_lookup_num = st.text_input(
            "Phone number (any format)",
            placeholder="e.g. +1 (732) 691-7161  or  7326917161",
            key="tcpa_lookup_num",
        )
        if st.button("Check TCPA", key="btn_tcpa_lookup"):
            if not admin_ok:
                st.error("Incorrect password.")
            elif not (TCPA_USER and TCPA_PASS):
                st.error("TCPA credentials are not configured on the server.")
            elif not tcpa_lookup_num.strip():
                st.warning("Enter a phone number.")
            else:
                normalised = clean_phone(tcpa_lookup_num.strip())
                if not normalised:
                    st.error("Could not parse a valid phone number from that input.")
                else:
                    st.write(f"Checking `{normalised}` (normalised from `{tcpa_lookup_num.strip()}`)…")
                    try:
                        results = scrub_tcpa_batch([normalised], TCPA_USER, TCPA_PASS)
                        result  = results.get(normalised, {})
                        if str(result.get("clean", "1")) == "0":
                            status_array = result.get("status_array") or []
                            mapping = {
                                "federal_dnc": "federal_dnc",
                                "state_dnc":   "state_dnc",
                                "tcpa":        "tcpa_litigator",
                                "complainers": "complainer",
                                "complainer":  "complainer",
                            }
                            if status_array:
                                vals = [mapping[v] for v in status_array if v in mapping]
                            else:
                                vals = []
                                if result.get("on_federal_dnc") == "Y": vals.append("federal_dnc")
                                if result.get("on_state_dnc")   == "Y": vals.append("state_dnc")
                                if result.get("on_tcpa")        == "Y": vals.append("tcpa_litigator")
                                if result.get("on_complainers") == "Y": vals.append("complainer")
                            label = ";".join(vals) if vals else "flagged"
                            st.error(f"🚫 **Flagged** — `{label}`")
                        elif result:
                            st.success("✅ **Clean** — not found on any TCPA list.")
                        else:
                            st.warning("No result returned for this number.")
                    except Exception as exc:
                        st.error(f"API error: {exc}")
