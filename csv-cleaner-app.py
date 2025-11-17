import os, re, io, gc, tempfile
import pandas as pd
import tldextract
import streamlit as st
from zipfile import ZipFile
from datetime import datetime

# ============================================================
#              CASE-INSENSITIVE COLUMN FINDER
# ============================================================
def find_col(df, patterns):
    for col in df.columns:
        lc = col.lower()
        for p in patterns:
            if p in lc:
                return col  # return original column name
    return None


# ============================================================
#              CLEANING HELPERS
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
#      STRONG SUPPRESSION EMAIL NORMALIZER (IMPORTANT)
# ============================================================
def normalize_suppression_email(e):
    if pd.isna(e): return None
    e = str(e).strip().lower()
    e = re.sub(r"[\"'\s]", "", e)          # remove spaces & quotes
    e = re.sub(r"^email[:\-]*", "", e)     # remove "email:" prefixes
    return e


# ============================================================
#          LOAD SUPPRESSION DATA (ROBUST)
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
                elif "domain" in lc or "website" in lc or "url" in lc:
                    domains.update(df[c].dropna().map(clean_domain))
                    found.append(c)

            logs.append(f"✅ {f.name}: found {', '.join(found) if found else 'no usable columns'}")

        except Exception as e:
            logs.append(f"⚠️ {f.name} skipped: {e}")

    return {"emails": emails, "phones": phones, "domains": domains, "logs": logs}


# ============================================================
#           HYBRID CLEAN ONE CHUNK (IMPORTANT)
# ============================================================
def clean_chunk(df, suppression):

    removed_email = 0
    removed_phone = 0
    removed_domain = 0

    # ---- Primary strict detection ----
    strict_email_col = find_col(df, ["email"])

    # ---- Legacy fallback detection ----
    fallback_email_cols = [c for c in df.columns if "email" in c.lower()]

    # ---- Merge ----
    email_cols = []
    if strict_email_col:
        email_cols.append(strict_email_col)
    for c in fallback_email_cols:
        if c not in email_cols:
            email_cols.append(c)

    # ---- Clean ALL email-like columns ----
    for ec in email_cols:
        df["__email"] = df[ec].map(clean_email)
        before = len(df)
        df = df[~df["__email"].isin(suppression["emails"])]
        removed_email += before - len(df)

    # ---- Phone ----
    phone_cols = [c for c in df.columns if "phone" in c.lower()]
    for pc in phone_cols:
        df["__phone"] = df[pc].map(clean_phone)
        before = len(df)
        df = df[~df["__phone"].isin(suppression["phones"])]
        removed_phone += before - len(df)

    # ---- Domain ----
    domain_cols = [
        c for c in df.columns
        if ("domain" in c.lower() or "website" in c.lower() or "url" in c.lower())
    ]
    for dc in domain_cols:
        df["__domain"] = df[dc].map(clean_domain)
        before = len(df)
        df = df[~df["__domain"].isin(suppression["domains"])]
        removed_domain += before - len(df)

    cleaned_df = df[[c for c in df.columns if not c.startswith("__")]]

    return cleaned_df, removed_email, removed_phone, removed_domain


# ============================================================
#             PROCESS CLEANING FILES (CHUNKED)
# ============================================================
def process_files(files_to_clean, suppression):
    summary, logs, outputs = [], [], {}

    for f in files_to_clean:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        first = True

        rows_before = 0
        cols_found = []
        removed_email_total = 0
        removed_phone_total = 0
        removed_domain_total = 0

        try:
            for chunk in pd.read_csv(f, dtype=str, chunksize=50000):

                rows_before += len(chunk)

                # capture actual column names
                for c in chunk.columns:
                    cl = c.lower()
                    if "email" in cl:  cols_found.append(c)
                    if "phone" in cl:  cols_found.append(c)
                    if "domain" in cl or "website" in cl or "url" in cl:
                        cols_found.append(c)

                cleaned, rem_e, rem_p, rem_d = clean_chunk(chunk, suppression)

                removed_email_total  += rem_e
                removed_phone_total  += rem_p
                removed_domain_total += rem_d

                cleaned.to_csv(tmp.name, index=False, mode="a", header=first)
                first = False

                del chunk, cleaned
                gc.collect()

            total_removed = removed_email_total + removed_phone_total + removed_domain_total
            rows_after = rows_before - total_removed

            logs.append(f"✔ {f.name}: removed {total_removed} rows")

            summary.append({
                "File": f.name,
                "Identified Columns": ", ".join(sorted(set(cols_found))) or "None",
                "Rows Before": rows_before,
                "Rows After": rows_after,
                "Removed by Email": removed_email_total,
                "Removed by Phone": removed_phone_total,
                "Removed by Domain": removed_domain_total,
                "Total Removed": total_removed
            })

            with open(tmp.name, "r", encoding="utf-8") as result:
                outputs[f.name] = result.read()

        except Exception as e:
            logs.append(f"⚠️ {f.name} failed: {e}")

        finally:
            try:
                tmp.close()
                os.remove(tmp.name)
            except:
                pass

    return pd.DataFrame(summary), logs, outputs


# ============================================================
#                      STREAMLIT UI
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
        for log in suppression["logs"]:
            st.write(log)

        st.info("Processing cleaning files…")
        summary_df, logs, cleaned_data = process_files(clean_files, suppression)
        for log in logs:
            st.write(log)

        # ZIP progress
        st.info("Preparing ZIP…")
        progress_text = st.empty()
        progress_bar = st.progress(0)

        zip_buffer = io.BytesIO()
        file_count = len(cleaned_data) + 1

        with ZipFile(zip_buffer, "w") as zf:
            for i, (name, data) in enumerate(cleaned_data.items(), start=1):
                progress_text.text(f"Adding {name} ({i}/{file_count})…")
                zf.writestr(name, data)
                progress_bar.progress(int(i / file_count * 100))

            progress_text.text("Adding summary file…")
            zf.writestr("_Cleaning_Summary.csv", summary_df.to_csv(index=False))
            progress_bar.progress(100)

        zip_buffer.seek(0)
        progress_text.text("✅ ZIP ready!")

        st.subheader("📊 Summary")
        st.dataframe(summary_df)

        st.warning("⚠ After clicking download, the browser may pause briefly — this is normal.")

        st.download_button(
            "⬇️ Download Cleaned Files (ZIP)",
            data=zip_buffer,
            file_name="Cleaned_Files.zip",
            mime="application/zip"
        )

        st.success("✨ Cleaning complete!")
        st.write(f"⏱ Total time: {datetime.now() - start}")
