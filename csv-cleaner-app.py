import os, re, io, gc, tempfile
import pandas as pd
import tldextract
import streamlit as st
from zipfile import ZipFile
from datetime import datetime

# ---------- Cleaning helpers ----------
def clean_email(email):
    if pd.isna(email): return None
    return str(email).strip().lower()

def clean_phone(phone):
    if pd.isna(phone): return None
    return re.sub(r"\D", "", str(phone))

def clean_domain(value):
    if pd.isna(value): return None
    value = str(value).strip().lower()
    ext = tldextract.extract(value)
    if not ext.domain: return None
    return f"{ext.domain}.{ext.suffix}"

# ---------- Load suppression data ----------
def load_suppression_data(files):
    emails, phones, domains = set(), set(), set()
    logs = []

    for f in files:
        try:
            df = pd.read_csv(f, dtype=str, nrows=200000)   # limit to first 200k lines per file
            found = []
            for c in df.columns:
                lc = c.lower()
                if "email" in lc:
                    emails.update(df[c].dropna().map(clean_email))
                    found.append("Email")
                elif "phone" in lc:
                    phones.update(df[c].dropna().map(clean_phone))
                    found.append("Phone")
                elif "domain" in lc or "website" in lc:
                    domains.update(df[c].dropna().map(clean_domain))
                    found.append("Domain")
            logs.append(f"✅ {f.name}: found {', '.join(found) if found else 'no relevant columns'}")
        except Exception as e:
            logs.append(f"⚠️ {f.name} skipped: {e}")
    return {"emails": emails, "phones": phones, "domains": domains, "logs": logs}

# ---------- Clean one chunk ----------
def clean_chunk(df, suppression):
    if "Email" in df.columns:
        df["Email_clean"] = df["Email"].map(clean_email)
        df = df[~df["Email_clean"].isin(suppression["emails"])]
    if "Phone" in df.columns:
        df["Phone_clean"] = df["Phone"].map(clean_phone)
        df = df[~df["Phone_clean"].isin(suppression["phones"])]
    dom_col = next((c for c in df.columns if "domain" in c.lower() or "website" in c.lower()), None)
    if dom_col:
        df["Domain_clean"] = df[dom_col].map(clean_domain)
        df = df[~df["Domain_clean"].isin(suppression["domains"])]
    return df[[c for c in df.columns if not c.endswith("_clean")]]

# ---------- Process all cleaning files ----------
def process_files(files_to_clean, suppression):
    summary, logs, outputs = [], [], {}
    for f in files_to_clean:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        first = True
        removed_total = 0
        rows_before = 0
        cols_found = []

        try:
            for chunk in pd.read_csv(f, dtype=str, chunksize=50000):
                rows_before += len(chunk)
                if any("email" in c.lower() for c in chunk.columns): cols_found.append("Email")
                if any("phone" in c.lower() for c in chunk.columns): cols_found.append("Phone")
                if any("domain" in c.lower() or "website" in c.lower() for c in chunk.columns): cols_found.append("Domain")
                before = len(chunk)
                cleaned = clean_chunk(chunk, suppression)
                removed_total += before - len(cleaned)
                cleaned.to_csv(tmp.name, index=False, mode="a", header=first)
                first = False
                del chunk, cleaned
                gc.collect()
            logs.append(f"✔ {f.name} — removed {removed_total} rows")
            summary.append({
                "File": f.name,
                "Identified Columns": ", ".join(sorted(set(cols_found))) or "None",
                "Rows Before": rows_before,
                "Removed Total": removed_total,
                "Rows After": rows_before - removed_total
            })
            with open(tmp.name, "r", encoding="utf-8") as result:
                outputs[f.name] = result.read()
        except Exception as e:
            logs.append(f"⚠️ {f.name} failed: {e}")
        finally:
            os.unlink(tmp.name)
    return pd.DataFrame(summary), logs, outputs

# ---------- Streamlit UI ----------
st.set_page_config(page_title="CSV Cleaner", layout="wide")
st.title("🧹 CSV Cleaner")
st.caption("Upload suppression lists and CSVs to clean.")

st.subheader("1️⃣ Upload Suppression Files")
sup_files = st.file_uploader("Upload one or more suppression CSVs", type="csv", accept_multiple_files=True, key="sup")

st.subheader("2️⃣ Upload Files to Clean")
clean_files = st.file_uploader("Upload one or more CSVs to clean", type="csv", accept_multiple_files=True, key="cln")

if st.button("Run Cleaning"):
    if not sup_files or not clean_files:
        st.error("Please upload both suppression and cleaning files.")
    else:
        start = datetime.now()
        st.info("Loading suppression data…")
        suppression = load_suppression_data(sup_files)
        for l in suppression["logs"]: st.write(l)

        st.info("Processing uploaded files (chunked)…")
        summary_df, logs, cleaned_data = process_files(clean_files, suppression)
        for l in logs: st.write(l)

        # Build ZIP for download
        zip_buffer = io.BytesIO()
        with ZipFile(zip_buffer, "w") as zf:
            for name, data in cleaned_data.items():
                zf.writestr(name, data)
            summary_csv = summary_df.to_csv(index=False)
            zf.writestr("_Cleaning_Summary.csv", summary_csv)
        zip_buffer.seek(0)


        # Build ZIP for download with visible progress
        st.info("Preparing download… please wait while files are compressed.")
        progress_text = st.empty()
        progress_bar = st.progress(0)
        
        zip_buffer = io.BytesIO()
        file_count = len(cleaned_data) + 1  # +1 for summary file
        
        with ZipFile(zip_buffer, "w") as zf:
            for i, (name, data) in enumerate(cleaned_data.items(), start=1):
                progress_text.text(f"Adding {name} ({i}/{file_count})…")
                zf.writestr(name, data)
                progress_bar.progress(int(i / file_count * 100))
            # write summary at the end
            progress_text.text("Adding summary file…")
            summary_csv = summary_df.to_csv(index=False)
            zf.writestr("_Cleaning_Summary.csv", summary_csv)
            progress_bar.progress(100)
        
        zip_buffer.seek(0)
        progress_text.text("✅ ZIP ready for download.")


        st.success("✅ Cleaning complete!")
        st.write(f"⏱ Duration: {datetime.now() - start}")
        st.subheader("📊 Summary")
        st.dataframe(summary_df)
        st.download_button(
            "⬇️ Download Cleaned Files (ZIP)",
            data=zip_buffer,
            file_name="Cleaned_Files.zip",
            mime="application/zip"
        )
