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


# ============================================================
# SAVE UPLOADED FILE TO DISK (crucial for memory safety)
# ============================================================
def save_uploaded_to_disk(uploaded_file):
    suffix = os.path.splitext(uploaded_file.name)[1] or ".csv"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(uploaded_file.getbuffer())
    tmp_path = tmp.name
    tmp.close()
    return tmp_path


# ============================================================
# CASE-INSENSITIVE COLUMN FINDER
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
    return digits if digits else None

def clean_domain(value):
    if pd.isna(value): return None
    ext = tldextract.extract(str(value).strip().lower())
    if not ext.domain: return None
    return f"{ext.domain}.{ext.suffix}"


# ============================================================
# NORMALIZE SUPPRESSION EMAILS
# ============================================================
def normalize_suppression_email(e):
    if pd.isna(e): return None
    e = str(e).strip().lower()
    e = re.sub(r"[\"'\s]", "", e)
    e = re.sub(r"^email[:\-]*", "", e)
    return e


# ============================================================
# LOAD SUPPRESSION DATA (small and safe)
# ============================================================
def load_suppression_data(files):
    emails, phones, domains = set(), set(), set()
    logs = []

    for f in files:
        try:
            df = pd.read_csv(f, dtype=str, nrows=200000)
            found = []

            for c in df.columns:
                lc = c.lower()
                if "email" in lc:
                    emails.update(df[c].dropna().map(normalize_suppression_email))
                    found.append(c)
                elif "phone" in lc:
                    phones.update(df[c].dropna().map(clean_phone))
                    found.append(c)
                elif any(x in lc for x in ["domain", "website", "url"]):
                    domains.update(df[c].dropna().map(clean_domain))
                    found.append(c)

            logs.append(f"✅ {getattr(f,'name',f)}: found {', '.join(found) if found else 'no usable columns'}")

        except Exception as e:
            logs.append(f"⚠️ {getattr(f,'name',f)} skipped: {e}")

    emails.discard(None)
    phones.discard(None)
    domains.discard(None)
    return {"emails": emails, "phones": phones, "domains": domains, "logs": logs}


# ============================================================
# CLEAN ONE CHUNK (DNC suppression)
# ============================================================
def clean_chunk(df, suppression):
    removed_email = removed_phone = removed_domain = 0

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
        df["__email"] = df[col].map(clean_email)
        before = len(df)
        df = df[~df["__email"].isin(suppression["emails"])]
        removed_email += before - len(df)

    # ---- Phone ----
    phone_cols = [c for c in df.columns if "phone" in c.lower()]
    for col in phone_cols:
        df["__phone"] = df[col].map(clean_phone)
        before = len(df)
        df = df[~df["__phone"].isin(suppression["phones"])]
        removed_phone += before - len(df)

    # ---- Domain ----
    domain_cols = [c for c in df.columns if any(x in c.lower() for x in ["domain", "website", "url"])]
    for col in domain_cols:
        df["__domain"] = df[col].map(clean_domain)
        before = len(df)
        df = df[~df["__domain"].isin(suppression["domains"])]
        removed_domain += before - len(df)

    df = df[[c for c in df.columns if not c.startswith("__")]]
    return df, removed_email, removed_phone, removed_domain


# ============================================================
# MEMORY-SAFE DNC PROCESSOR
# ============================================================
def process_files(files_to_clean, suppression):
    summary, logs = [], []
    cleaned_paths = {}

    total_files  = len(files_to_clean)
    global_bar   = st.progress(0)
    global_status = st.empty()

    for file_index, uploaded in enumerate(files_to_clean, start=1):
        global_status.write(f"Processing {uploaded.name} ({file_index}/{total_files})")
        global_bar.progress(int((file_index - 1) / total_files * 100))

        source_path = save_uploaded_to_disk(uploaded)

        out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        out_path = out_tmp.name
        out_tmp.close()

        first_write = True
        rows_before = 0
        cols_found  = []
        removed_email_total = removed_phone_total = removed_domain_total = 0

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

                cleaned, rem_e, rem_p, rem_d = clean_chunk(chunk, suppression)
                removed_email_total  += rem_e
                removed_phone_total  += rem_p
                removed_domain_total += rem_d

                cleaned.to_csv(out_path, index=False, mode="a", header=first_write)
                first_write = False

                file_bar.progress(min(100, chunk_counter * 5))
                file_status.write(f"{uploaded.name}: processed {chunk_counter} chunks…")

                del chunk, cleaned
                gc.collect()

            total_removed = removed_email_total + removed_phone_total + removed_domain_total
            rows_after    = rows_before - total_removed
            logs.append(f"✔ {uploaded.name}: removed {total_removed} rows (DNC)")

            summary.append({
                "File":               uploaded.name,
                "Identified Columns": ", ".join(sorted(set(cols_found))) or "None",
                "Rows Before":        rows_before,
                "Removed by Email":   removed_email_total,
                "Removed by Phone":   removed_phone_total,
                "Removed by Domain":  removed_domain_total,
                "Removed by TCPA":    0,          # filled in later if TCPA scrub runs
                "Total Removed":      total_removed,
                "Rows After":         rows_after,
            })
            cleaned_paths[uploaded.name] = out_path

        except Exception as e:
            logs.append(f"⚠️ {uploaded.name} failed: {e}")

        finally:
            try: os.remove(source_path)
            except: pass

    global_bar.progress(100)
    global_status.write("DNC cleaning complete.")
    return pd.DataFrame(summary), logs, cleaned_paths


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


def collect_phones_from_files(cleaned_paths):
    """Read all cleaned temp files and return a set of unique normalised phone numbers."""
    all_phones: set[str] = set()
    for path in cleaned_paths.values():
        try:
            for chunk in pd.read_csv(path, dtype=str, chunksize=CHUNK_SIZE,
                                     engine="python", on_bad_lines="skip"):
                phone_cols = [c for c in chunk.columns if "phone" in c.lower()]
                for col in phone_cols:
                    all_phones.update(
                        chunk[col].dropna().map(clean_phone).dropna()
                    )
        except Exception:
            pass
    all_phones.discard(None)
    all_phones.discard("")
    return all_phones


def tcpa_scrub_cleaned_files(cleaned_paths, summary_df, user, password):
    """
    1. Collect all unique phones from DNC-cleaned files.
    2. Scrub against TCPA API in batches.
    3. Remove flagged rows from each file (in-place replace).
    4. Update summary_df with Removed by TCPA column.
    """
    st.info("Collecting phone numbers from cleaned files…")
    all_phones = collect_phones_from_files(cleaned_paths)
    phone_list = list(all_phones)
    st.write(f"  {len(phone_list):,} unique phone numbers to scrub")

    # ── Scrub ──────────────────────────────────────────────────────────────────
    flagged_phones: set[str] = set()
    total_batches = (len(phone_list) + TCPA_BATCH_SIZE - 1) // TCPA_BATCH_SIZE
    tcpa_bar    = st.progress(0)
    tcpa_status = st.empty()

    for i, start in enumerate(range(0, len(phone_list), TCPA_BATCH_SIZE), 1):
        batch = phone_list[start : start + TCPA_BATCH_SIZE]
        tcpa_status.write(f"TCPA scrub: batch {i}/{total_batches} ({len(batch)} numbers)…")
        try:
            results = scrub_tcpa_batch(batch, user, password)
            for phone, result in results.items():
                if str(result.get("clean", "1")) == "0":
                    flagged_phones.add(phone)
        except Exception as exc:
            st.warning(f"Batch {i} failed: {exc}")
        tcpa_bar.progress(int(i / total_batches * 100))

    tcpa_bar.progress(100)
    st.write(f"  {len(flagged_phones):,} phone numbers flagged by TCPA")

    if not flagged_phones:
        st.write("  No TCPA-flagged numbers — no rows removed.")
        return summary_df

    # ── Remove flagged rows from each file ────────────────────────────────────
    for name, path in list(cleaned_paths.items()):
        out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        out_path = out_tmp.name
        out_tmp.close()

        first_write  = True
        tcpa_removed = 0

        try:
            for chunk in pd.read_csv(path, dtype=str, chunksize=CHUNK_SIZE,
                                     engine="python", on_bad_lines="skip"):
                phone_cols = [c for c in chunk.columns if "phone" in c.lower()]
                before = len(chunk)
                for col in phone_cols:
                    chunk["__phone"] = chunk[col].map(clean_phone)
                    chunk = chunk[~chunk["__phone"].isin(flagged_phones)]
                chunk = chunk[[c for c in chunk.columns if not c.startswith("__")]]
                tcpa_removed += before - len(chunk)
                chunk.to_csv(out_path, index=False, mode="a", header=first_write)
                first_write = False
                del chunk
                gc.collect()

            os.remove(path)
            cleaned_paths[name] = out_path

            # Update summary row
            mask = summary_df["File"] == name
            summary_df.loc[mask, "Removed by TCPA"] = tcpa_removed
            summary_df.loc[mask, "Total Removed"]   += tcpa_removed
            summary_df.loc[mask, "Rows After"]      -= tcpa_removed

            st.write(f"  ✔ {name}: {tcpa_removed:,} rows removed by TCPA")

        except Exception as exc:
            st.warning(f"  ⚠️ {name} TCPA pass failed: {exc}")
            try: os.remove(out_path)
            except: pass

    return summary_df


# ============================================================
# STREAMLIT UI
# ============================================================
st.set_page_config(page_title="CSV Cleaner", layout="wide")
st.title("🧹 CSV Cleaner")

st.subheader("1️⃣ Upload Suppression Files")
sup_files = st.file_uploader(
    "Upload suppression CSV files", type="csv", accept_multiple_files=True
)

st.subheader("2️⃣ Upload Files to Clean")
clean_files = st.file_uploader(
    "Upload CSV files to clean", type="csv", accept_multiple_files=True
)

st.subheader("3️⃣ TCPA Phone Scrub (Optional)")
enable_tcpa = st.checkbox("Also scrub phone numbers against TCPA Litigator List", value=True)

if st.button("Run Cleaning"):
    if not sup_files or not clean_files:
        st.error("Please upload both suppression and cleaning files.")
    elif enable_tcpa and not (TCPA_USER and TCPA_PASS):
        st.error("TCPA credentials are not configured on the server. Contact the administrator.")
    else:
        start = datetime.now()

        st.info("Loading suppression data…")
        suppression = load_suppression_data(sup_files)
        for log in suppression["logs"]:
            st.write(log)

        st.info("Running DNC suppression…")
        summary_df, logs, cleaned_paths = process_files(clean_files, suppression)
        for log in logs:
            st.write(log)

        if enable_tcpa:
            st.info("Running TCPA phone scrub…")
            summary_df = tcpa_scrub_cleaned_files(
                cleaned_paths, summary_df, TCPA_USER, TCPA_PASS
            )

        st.info("Preparing ZIP…")
        zip_buffer = io.BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            for name, path in cleaned_paths.items():
                zf.write(path, arcname=name)
            zf.writestr("_Cleaning_Summary.csv", summary_df.to_csv(index=False))
        zip_buffer.seek(0)

        for p in cleaned_paths.values():
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
