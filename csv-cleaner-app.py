import os, re, io, gc, tempfile
import pandas as pd
import tldextract
import streamlit as st
from zipfile import ZipFile
from datetime import datetime


CHUNK_SIZE = 50000  # split threshold


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
    return re.sub(r"\D", "", str(phone))

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

    return {"emails": emails, "phones": phones, "domains": domains, "logs": logs}


# ============================================================
# CLEAN ONE CHUNK
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
# MEMORY-SAFE PROCESSOR
# ============================================================
def process_files(files_to_clean, suppression):
    summary, logs = [], []
    cleaned_paths = {}   # {filename: temp_path}

    total_files = len(files_to_clean)
    global_bar = st.progress(0)
    global_status = st.empty()

    for file_index, uploaded in enumerate(files_to_clean, start=1):
        global_status.write(f"Processing {uploaded.name} ({file_index}/{total_files})")
        global_bar.progress(int((file_index - 1) / total_files * 100))

        # 1. Save uploaded file to disk
        source_path = save_uploaded_to_disk(uploaded)

        # 2. Temp cleaned output file
        out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        out_path = out_tmp.name
        out_tmp.close()

        first_write = True
        rows_before = 0
        cols_found = []
        removed_email_total = removed_phone_total = removed_domain_total = 0

        # Per-file progress
        file_bar = st.progress(0)
        file_status = st.empty()
        chunk_counter = 0

        try:
            for chunk in pd.read_csv(
                source_path,
                dtype=str,
                chunksize=CHUNK_SIZE,
                sep=",",
                engine="python",
                on_bad_lines="skip"
            ):
                chunk_counter += 1
                rows_before += len(chunk)

                # Identify columns
                for c in chunk.columns:
                    lc = c.lower()
                    if "email" in lc: cols_found.append(c)
                    if "phone" in lc: cols_found.append(c)
                    if any(x in lc for x in ["domain", "website", "url"]): cols_found.append(c)

                cleaned, rem_e, rem_p, rem_d = clean_chunk(chunk, suppression)

                removed_email_total += rem_e
                removed_phone_total += rem_p
                removed_domain_total += rem_d

                cleaned.to_csv(out_path, index=False, mode="a", header=first_write)
                first_write = False

                # update chunk progress
                file_bar.progress(min(100, chunk_counter * 5))
                file_status.write(f"{uploaded.name}: processed {chunk_counter} chunks…")

                del chunk, cleaned
                gc.collect()

            total_removed = removed_email_total + removed_phone_total + removed_domain_total
            rows_after = rows_before - total_removed

            logs.append(f"✔ {uploaded.name}: removed {total_removed} rows")

            summary.append({
                "File": uploaded.name,
                "Identified Columns": ", ".join(sorted(set(cols_found))) or "None",
                "Rows Before": rows_before,
                "Rows After": rows_after,
                "Removed by Email": removed_email_total,
                "Removed by Phone": removed_phone_total,
                "Removed by Domain": removed_domain_total,
                "Total Removed": total_removed
            })

            cleaned_paths[uploaded.name] = out_path

        except Exception as e:
            logs.append(f"⚠️ {uploaded.name} failed: {e}")

        finally:
            # clean up uploaded temp file
            try: os.remove(source_path)
            except: pass

    global_bar.progress(100)
    global_status.write("All files processed.")
    return pd.DataFrame(summary), logs, cleaned_paths


# ============================================================
# STREAMLIT UI
# ============================================================
st.set_page_config(page_title="CSV Cleaner", layout="wide")
st.title("🧹 CSV Cleaner")

st.subheader("1️⃣ Upload Suppression Files")
sup_files = st.file_uploader("Upload suppression CSV files", type="csv", accept_multiple_files=True)

st.subheader("2️⃣ Upload Files to Clean")
clean_files = st.file_uploader("Upload CSV files to clean", type="csv", accept_multiple_files=True)

if st.button("Run Cleaning"):
    if not sup_files or not clean_files:
        st.error("Please upload both suppression and cleaning files.")
    else:
        start = datetime.now()

        st.info("Loading suppression data…")
        suppression = load_suppression_data(sup_files)
        for log in suppression["logs"]: st.write(log)

        st.info("Processing files…")
        summary_df, logs, cleaned_paths = process_files(clean_files, suppression)
        for log in logs: st.write(log)

        # Build ZIP from disk (memory-safe)
        st.info("Preparing ZIP…")
        zip_buffer = io.BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            for name, path in cleaned_paths.items():
                zf.write(path, arcname=name)
            zf.writestr("_Cleaning_Summary.csv", summary_df.to_csv(index=False))

        zip_buffer.seek(0)

        # Cleanup cleaned files
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

        st.success(f"✨ Cleaning complete! Total time: {datetime.now() - start}")
