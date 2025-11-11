import os
import re
import io
import pandas as pd
import tldextract
import streamlit as st
from zipfile import ZipFile
from datetime import datetime

# =========================================
# Helper cleaning functions
# =========================================

def clean_email(email):
    if pd.isna(email):
        return None
    return str(email).strip().lower()

def clean_phone(phone):
    if pd.isna(phone):
        return None
    return re.sub(r"\D", "", str(phone))

def clean_domain(value):
    if pd.isna(value):
        return None
    value = str(value).strip().lower()
    ext = tldextract.extract(value)
    if not ext.domain:
        return None
    return f"{ext.domain}.{ext.suffix}"

# =========================================
# Load suppression data from uploaded files
# =========================================

def load_suppression_data(uploaded_files):
    emails, phones, domains = set(), set(), set()
    logs = []

    for file in uploaded_files:
        try:
            df = pd.read_csv(file, dtype=str)
            found_cols = []
            for col in df.columns:
                c = col.lower()
                if "email" in c:
                    found_cols.append("Email")
                    emails.update(df[col].dropna().map(clean_email))
                elif "phone" in c:
                    found_cols.append("Phone")
                    phones.update(df[col].dropna().map(clean_phone))
                elif "domain" in c or "website" in c:
                    found_cols.append("Domain")
                    domains.update(df[col].dropna().map(clean_domain))
            logs.append(f"✅ {file.name} — found {', '.join(found_cols) if found_cols else 'no relevant columns'}")
        except Exception as e:
            logs.append(f"⚠️ Skipped {file.name}: {e}")

    return {
        "emails": {e for e in emails if e},
        "phones": {p for p in phones if p},
        "domains": {d for d in domains if d},
        "logs": logs
    }

# =========================================
# Clean files uploaded for processing
# =========================================

def clean_files(files_to_clean, suppression_data):
    summary_records = []
    cleaned_outputs = {}
    logs = []

    for file in files_to_clean:
        try:
            df = pd.read_csv(file, dtype=str)
            before_rows = len(df)
            removed_email = removed_phone = removed_domain = 0
            found_cols = []

            # Email
            if "Email" in df.columns:
                found_cols.append("Email")
                df["Email_clean"] = df["Email"].map(clean_email)
                mask = df["Email_clean"].isin(suppression_data["emails"])
                removed_email = mask.sum()
                df = df[~mask]

            # Phone
            if "Phone" in df.columns:
                found_cols.append("Phone")
                df["Phone_clean"] = df["Phone"].map(clean_phone)
                mask = df["Phone_clean"].isin(suppression_data["phones"])
                removed_phone = mask.sum()
                df = df[~mask]

            # Domain / Website
            domain_col = None
            for c in df.columns:
                if "domain" in c.lower() or "website" in c.lower():
                    domain_col = c
                    break
            if domain_col:
                found_cols.append(domain_col)
                df["Domain_clean"] = df[domain_col].map(clean_domain)
                mask = df["Domain_clean"].isin(suppression_data["domains"])
                removed_domain = mask.sum()
                df = df[~mask]

            after_rows = len(df)
            df = df[[c for c in df.columns if not c.endswith("_clean")]]

            summary_records.append({
                "File": file.name,
                "Identified Columns": ", ".join(found_cols) if found_cols else "None",
                "Rows Before": before_rows,
                "Rows After": after_rows,
                "Removed by Email": removed_email,
                "Removed by Phone": removed_phone,
                "Removed by Domain": removed_domain,
                "Total Removed": before_rows - after_rows
            })

            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            cleaned_outputs[file.name] = buffer.getvalue()
            logs.append(f"✔ Processed {file.name}: {before_rows - after_rows} removed")

        except Exception as e:
            logs.append(f"⚠️ Skipped {file.name}: {e}")

    summary_df = pd.DataFrame(summary_records)
    csv_buffer = io.StringIO()
    summary_df.to_csv(csv_buffer, index=False)
    cleaned_outputs["_Cleaning_Summary.csv"] = csv_buffer.getvalue()

    zip_buffer = io.BytesIO()
    with ZipFile(zip_buffer, "w") as zf:
        for name, data in cleaned_outputs.items():
            zf.writestr(name, data)
    zip_buffer.seek(0)

    logs.append("📊 Cleaning complete. All files zipped for download.")
    return summary_df, logs, zip_buffer

# =========================================
# Streamlit Interface
# =========================================

st.set_page_config(page_title="CSV Cleaner", layout="wide")
st.title("🧹 CSV Cleaner Web App")
st.caption("Upload suppression files (emails, phones, domains) and files to clean. The app will remove matching rows and generate cleaned CSVs.")

st.subheader("1️⃣ Upload Suppression Files")
suppression_files = st.file_uploader(
    "Upload one or more suppression CSV files",
    type="csv",
    accept_multiple_files=True,
    key="suppression"
)

st.subheader("2️⃣ Upload Files to Clean")
files_to_clean = st.file_uploader(
    "Upload one or more CSVs to clean",
    type="csv",
    accept_multiple_files=True,
    key="toclean"
)

if st.button("Run Cleaning"):
    if not suppression_files:
        st.error("Please upload at least one suppression file.")
    elif not files_to_clean:
        st.error("Please upload at least one file to clean.")
    else:
        start_time = datetime.now()
        st.info("Loading suppression data...")
        suppression_data = load_suppression_data(suppression_files)
        for msg in suppression_data["logs"]:
            st.write(msg)

        st.info("Cleaning files...")
        summary_df, logs, zip_buffer = clean_files(files_to_clean, suppression_data)
        for msg in logs:
            st.write(msg)

        st.success("✅ All done!")
        st.write(f"⏱ Duration: {datetime.now() - start_time}")

        st.subheader("📊 Summary Table")
        st.dataframe(summary_df)

        st.download_button(
            label="⬇️ Download Cleaned Files (ZIP)",
            data=zip_buffer,
            file_name="Cleaned_Files.zip",
            mime="application/zip"
        )
