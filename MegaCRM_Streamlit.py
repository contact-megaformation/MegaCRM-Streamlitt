# FinanceOnly_Streamlit.py
# 💸 Streamlit — Revenus/Dépenses فقط (Admin + Employés) مع Google Sheets

import json, time, urllib.parse
import streamlit as st
import pandas as pd
import gspread
import gspread.exceptions as gse
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
from PIL import Image

# ================== إعداد الصفحة ==================
st.set_page_config(page_title="Finance MEGA", layout="wide", initial_sidebar_state="expanded")
st.markdown(
    """
    <div style='text-align:center'>
      <h1>💸 Finance — Revenus & Dépenses</h1>
      <p>نسخة مبسّطة للموظّفين والأدمِن</p>
    </div>
    <hr/>
    """, unsafe_allow_html=True
)

# ================== Google Auth ==================
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

def make_client_and_sheet_id():
    try:
        sa = st.secrets["gcp_service_account"]
        sa_info = dict(sa) if hasattr(sa, "keys") else (json.loads(sa) if isinstance(sa, str) else {})
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = st.secrets["SPREADSHEET_ID"]
        return client, sheet_id
    except Exception:
        creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = "PUT_YOUR_SHEET_ID_HERE"
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

# ================== ثوابت ==================
FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]

FIN_REV_COLUMNS = [
    "Date","Libellé","Prix",
    "Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total",
    "Echeance","Reste",
    "Mode","Employé","Catégorie","Note"
]
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"]

def fin_month_title(mois: str, kind: str, branch: str):
    prefix = "Revenue " if kind=="Revenus" else "Dépense "
    short  = "MB" if "Menzel" in branch else "BZ"
    return f"{prefix}{mois} ({short})"

def fmt_date(d: date|None) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def _to_num_series_any(s):
    return (
        pd.Series(s).astype(str)
        .str.replace(" ","",regex=False)
        .str.replace(",",".",regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )

def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {"Menzel Bourguiba": str(b.get("MB","MB_2025!")), "Bizerte": str(b.get("BZ","BZ_2025!"))}
    except Exception:
        return {"Menzel Bourguiba":"MB_2025!","Bizerte":"BZ_2025!"}

def emp_pwd_for(name:str)->str:
    try:
        mp = st.secrets["employee_passwords"]
        return str(mp.get(name, mp.get("_default","1234")))
    except Exception:
        return "1234"

# ================== Sheets Helpers (Backoff + Cache) ==================
def get_spreadsheet():
    if st.session_state.get("sh_id") == SPREADSHEET_ID and "sh_obj" in st.session_state:
        return st.session_state["sh_obj"]
    last_err = None
    for i in range(5):
        try:
            sh = client.open_by_key(SPREADSHEET_ID)
            st.session_state["sh_obj"] = sh
            st.session_state["sh_id"]  = SPREADSHEET_ID
            return sh
        except gse.APIError as e:
            last_err = e
            time.sleep(0.5 * (2**i))
    st.error("تعذّر فتح Google Sheet (ممكن كوتا). حاول بعد شوية.")
    raise last_err

def ensure_ws(title: str, columns: list[str]):
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 8)))
        ws.update("1:1", [columns])
        return ws
    header = ws.row_values(1)
    if not header or header[:len(columns)] != columns:
        ws.update("1:1", [columns])
    return ws

@st.cache_data(ttl=120, show_spinner=False)
def _read_ws_all_values_cached(title: str, cols: tuple) -> list[list[str]]:
    ws = ensure_ws(title, list(cols))
    return ws.get_all_values()

def fin_read_df(title: str, kind: str) -> pd.DataFrame:
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    values = _read_ws_all_values_cached(title, tuple(cols))
    if not values:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(values[1:], columns=values[0] if values else cols)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)

    if kind == "Revenus":
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
            if c in df.columns: df[c] = _to_num_series_any(df[c])
        if "Echeance" in df.columns:
            df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)
        if "Alert" not in df.columns:
            df["Alert"] = ""
        if "Echeance" in df.columns and "Reste" in df.columns:
            today_ts = pd.Timestamp.now().normalize()
            ech = pd.to_datetime(df["Echeance"], errors="coerce")
            reste = pd.to_numeric(df["Reste"], errors="coerce").fillna(0.0)
            df.loc[ech.notna() & (ech < today_ts) & (reste > 0), "Alert"] = "⚠️ متأخر"
            df.loc[ech.notna() & (ech.dt.normalize() == today_ts) & (reste > 0), "Alert"] = "⏰ اليوم"
    else:
        if "Montant" in df.columns:
            df["Montant"] = _to_num_series_any(df["Montant"])
    return df

def fin_append_row(title: str, row: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind=="Revenus" else FIN_DEP_COLUMNS
    ws = ensure_ws(title, cols)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)
    _read_ws_all_values_cached.clear()

# ================== الشريط الجانبي ==================
try:
    st.sidebar.image(Image.open("logo.png"), use_container_width=True)
except Exception:
    pass

role = st.sidebar.radio("الدور", ["موظف","أدمن"], horizontal=True)

# قفل الأدمِن
def admin_unlocked() -> bool:
    ok = st.session_state.get("admin_ok", False)
    ts = st.session_state.get("admin_ok_at")
    return bool(ok and ts and (datetime.now()-ts)<=timedelta(minutes=30))

if role=="أدمن":
    with st.sidebar.expander("🔐 إدارة (Admin)"):
        if admin_unlocked():
            st.success("مفتوح (30 دقيقة)")
            if st.button("قفل"):
                st.session_state["admin_ok"]=False; st.session_state["admin_ok_at"]=None; st.rerun()
        else:
            admin_pwd = st.text_input("كلمة سرّ الأدمِن", type="password")
            if st.button("فتح"):
                conf = str(st.secrets.get("admin_password","admin123"))
                if admin_pwd and admin_pwd==conf:
                    st.session_state["admin_ok"]=True; st.session_state["admin_ok_at"]=datetime.now()
                    st.success("تم الفتح ✅")
                else:
                    st.error("كلمة سرّ غير صحيحة")

# تعريف الموظّف + قفل بسيط
if role=="موظف":
    with st.sidebar.expander("👤 الموظّف"):
        employee = st.text_input("اسمك (كما يظهر في العمود Employé)")
        emp_pw = st.text_input("كلمة السرّ", type="password")
        emp_ok = st.button("تأكيد الدخول")
        if emp_ok:
            if employee and emp_pw==emp_pwd_for(employee):
                st.session_state["emp_ok_name"]=employee
                st.session_state["emp_ok_at"]=datetime.now()
                st.success("تم الدخول ✅")
            else:
                st.error("اسم/كلمة سرّ غير صحيحة")
else:
    employee = None

def emp_unlocked()->bool:
    name = st.session_state.get("emp_ok_name")
    ts   = st.session_state.get("emp_ok_at")
    return bool(name and ts and (datetime.now()-ts)<=timedelta(minutes=20))

# إعدادات المالية المشتركة
st.sidebar.markdown("---")
branch = st.sidebar.selectbox("🏢 الفرع", ["Menzel Bourguiba","Bizerte"])
kind_ar = st.sidebar.radio("النوع", ["مداخيل","مصاريف"], horizontal=True)
kind    = "Revenus" if kind_ar=="مداخيل" else "Dépenses"
mois    = st.sidebar.selectbox("🗓️ الشهر", FIN_MONTHS_FR, index=datetime.now().month-1)

# قفل الفرع (اختياري لكن مفيد)
BRANCH_PASSWORDS = _branch_passwords()
key_pw = f"finance_pw_ok::{branch}"
if key_pw not in st.session_state:
    st.session_state[key_pw]=False
with st.sidebar.expander("🔒 قفل الفرع"):
    if st.session_state[key_pw]:
        st.success("الدخول للفرع ✅")
        if st.button("قفل الفرع"):
            st.session_state[key_pw]=False
    else:
        pw_try = st.text_input("كلمة سرّ الفرع", type="password", key=f"fin_pw_{branch}")
        if st.button("دخول الفرع", key=f"fin_enter_{branch}"):
            if pw_try == BRANCH_PASSWORDS.get(branch,""): st.session_state[key_pw]=True; st.success("OK ✅")
            else: st.error("غلط في كلمة السرّ")

if not st.session_state.get(key_pw, False):
    st.info("⬅️ أدخل كلمة سرّ الفرع للمتابعة.")
    st.stop()

# إذا موظّف: لازم يكون مفتوح
if role=="موظف" and not emp_unlocked():
    st.info("🔐 أدخل اسمك وكلمة السرّ من اليسار.")
    st.stop()

# ================== عرض البيانات ==================
st.header(f"📄 {fin_month_title(mois, kind, branch)}")
title = fin_month_title(mois, kind, branch)
df_fin = fin_read_df(title, kind)
df_view = df_fin.copy()

# الموظف يشوف/يزيد كان سطوره
if role=="موظف" and employee:
    df_view = df_view[df_view.get("Employé","").astype(str).str.strip().str.lower() == employee.strip().lower()]

# فلاتر
with st.expander("🔎 فلاتر"):
    c1,c2,c3 = st.columns(3)
    date_from = c1.date_input("من تاريخ", value=None)
    date_to   = c2.date_input("إلى تاريخ", value=None)
    search    = c3.text_input("بحث (Libellé/Catégorie/Mode/Note/Employé)")
    if "Date" in df_view.columns:
        if date_from: df_view = df_view[df_view["Date"] >= pd.to_datetime(date_from)]
        if date_to:   df_view = df_view[df_view["Date"] <= pd.to_datetime(date_to)]
    if search and not df_view.empty:
        m = pd.Series([False]*len(df_view))
        cols_search = ["Libellé","Catégorie","Mode","Employé","Note","Caisse_Source"]
        cols_search = [c for c in cols_search if c in df_view.columns]
        for c in cols_search:
            m |= df_view[c].fillna("").astype(str).str.contains(search, case=False, na=False)
        df_view = df_view[m]

# جدول العرض
if kind=="Revenus":
    cols_show = [c for c in ["Date","Libellé","Prix","Montant_Admin","Montant_Structure",
                             "Montant_PreInscription","Montant_Total","Echeance","Reste",
                             "Alert","Mode","Employé","Catégorie","Note"] if c in df_view.columns]
else:
    cols_show = [c for c in ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"] if c in df_view.columns]

st.dataframe(df_view[cols_show] if not df_view.empty else pd.DataFrame(columns=cols_show), use_container_width=True)

# ================== ملخّص شهري (Admin فقط) ==================
if role=="أدمن" and admin_unlocked():
    with st.expander("📊 ملخّص الفرع للشهر — Admin Only", expanded=False):
        rev_df = fin_read_df(fin_month_title(mois,"Revenus",branch), "Revenus")
        dep_df = fin_read_df(fin_month_title(mois,"Dépenses",branch), "Dépenses")

        sum_admin  = rev_df["Montant_Admin"].sum() if ("Montant_Admin" in rev_df) else 0.0
        sum_struct = rev_df["Montant_Structure"].sum() if ("Montant_Structure" in rev_df) else 0.0
        sum_preins = rev_df["Montant_PreInscription"].sum() if ("Montant_PreInscription" in rev_df) else 0.0
        sum_total_as = rev_df["Montant_Total"].sum() if ("Montant_Total" in rev_df) else (sum_admin+sum_struct)
        sum_reste_due= rev_df["Reste"].sum() if ("Reste" in rev_df) else 0.0

        if not dep_df.empty and "Caisse_Source" in dep_df and "Montant" in dep_df:
            dep_admin  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Admin","Montant"].sum()
            dep_struct = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Structure","Montant"].sum()
            dep_inscr  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Inscription","Montant"].sum()
        else:
            dep_admin=dep_struct=dep_inscr=0.0

        reste_admin  = float(sum_admin)  - float(dep_admin)
        reste_struct = float(sum_struct) - float(dep_struct)
        reste_inscr  = float(sum_preins) - float(dep_inscr)

        c1,c2,c3 = st.columns(3)
        c1.metric("مداخيل Admin", f"{sum_admin:,.2f}")
        c2.metric("مصاريف Admin", f"{dep_admin:,.2f}")
        c3.metric("Reste Admin", f"{reste_admin:,.2f}")
        s1,s2,s3 = st.columns(3)
        s1.metric("مداخيل Structure", f"{sum_struct:,.2f}")
        s2.metric("مصاريف Structure", f"{dep_struct:,.2f}")
        s3.metric("Reste Structure", f"{reste_struct:,.2f}")
        i1,i2,i3 = st.columns(3)
        i1.metric("مداخيل Inscription", f"{sum_preins:,.2f}")
        i2.metric("مصاريف Inscription", f"{dep_inscr:,.2f}")
        i3.metric("Reste Inscription", f"{reste_inscr:,.2f}")
        x1,x2,x3 = st.columns(3)
        x1.metric("Total Admin+Structure", f"{sum_total_as:,.2f}")
        x2.metric("Total مصاريف", f"{(dep_admin+dep_struct+dep_inscr):,.2f}")
        x3.metric("إجمالي Reste Due", f"{sum_reste_due:,.2f}")

    with st.expander("📆 ملخّص يومي Admin/Structure — Admin Only", expanded=False):
        # تجهيز داتا
        rev_df = fin_read_df(fin_month_title(mois,"Revenus",branch), "Revenus")
        dep_df = fin_read_df(fin_month_title(mois,"Dépenses",branch), "Dépenses")
        for dcol in ("Date",):
            if dcol in rev_df.columns: rev_df[dcol] = pd.to_datetime(rev_df[dcol], errors="coerce")
            if dcol in dep_df.columns: dep_df[dcol] = pd.to_datetime(dep_df[dcol], errors="coerce")
        if not dep_df.empty and "Montant" in dep_df: dep_df["Montant"] = _to_num_series_any(dep_df["Montant"])

        # تجميع
        rev_day = pd.DataFrame(index=pd.to_datetime([]))
        if not rev_df.empty and "Date" in rev_df.columns:
            grp_rev = rev_df.groupby(rev_df["Date"].dt.normalize()).agg(
                Rev_Admin=("Montant_Admin", "sum"),
                Rev_Structure=("Montant_Structure", "sum"),
            )
            rev_day = grp_rev

        dep_day = pd.DataFrame(index=pd.to_datetime([]))
        if not dep_df.empty and "Date" in dep_df.columns and "Caisse_Source" in dep_df.columns:
            dep_admin_day  = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Admin"].groupby(dep_df["Date"].dt.normalize())["Montant"].sum().rename("Dep_Admin")
            dep_struct_day = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Structure"].groupby(dep_df["Date"].dt.normalize())["Montant"].sum().rename("Dep_Structure")
            dep_day = pd.concat([dep_admin_day, dep_struct_day], axis=1)

        mois_idx = FIN_MONTHS_FR.index(mois) + 1
        today_year = datetime.now().year
        start = pd.Timestamp(today_year, mois_idx, 1)
        end = (start + pd.offsets.MonthEnd(1))
        full_range = pd.date_range(start, end, freq="D")

        daily = pd.DataFrame(index=full_range)
        if not rev_day.empty: daily = daily.join(rev_day, how="left")
        if not dep_day.empty: daily = daily.join(dep_day, how="left")
        for c in ["Rev_Admin","Rev_Structure","Dep_Admin","Dep_Structure"]:
            if c not in daily.columns: daily[c] = 0.0
            daily[c] = daily[c].fillna(0.0)

        daily["Reste_Admin_Journalier"]     = daily["Rev_Admin"]     - daily["Dep_Admin"]
        daily["Reste_Structure_Journalier"] = daily["Rev_Structure"] - daily["Dep_Structure"]
        daily["Reste_Admin_Cumulé"]     = (daily["Rev_Admin"]     - daily["Dep_Admin"]).cumsum()
        daily["Reste_Structure_Cumulé"] = (daily["Rev_Structure"] - daily["Dep_Structure"]).cumsum()

        daily = daily.reset_index().rename(columns={"index":"Date"})
        cols_order = [
            "Date",
            "Rev_Admin","Dep_Admin","Reste_Admin_Journalier","Reste_Admin_Cumulé",
            "Rev_Structure","Dep_Structure","Reste_Structure_Journalier","Reste_Structure_Cumulé",
        ]
        daily = daily[cols_order]

        st.dataframe(
            daily.style.format({
                "Rev_Admin": "{:,.2f}", "Dep_Admin": "{:,.2f}",
                "Reste_Admin_Journalier": "{:,.2f}", "Reste_Admin_Cumulé": "{:,.2f}",
                "Rev_Structure": "{:,.2f}", "Dep_Structure": "{:,.2f}",
                "Reste_Structure_Journalier": "{:,.2f}", "Reste_Structure_Cumulé": "{:,.2f}",
            }),
            use_container_width=True
        )
        csv_bytes = daily.to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ تنزيل CSV (الملخص اليومي)", data=csv_bytes,
                           file_name=f"daily_summary_{branch}_{mois}.csv", mime="text/csv")

# ================== إضافة عملية جديدة ==================
st.markdown("---")
st.subheader("➕ إضافة عملية جديدة")

with st.form("fin_add_row"):
    d1,d2,d3 = st.columns(3)
    date_val = d1.date_input("Date", value=datetime.today())
    libelle  = d2.text_input("Libellé")
    employe  = d3.text_input("Employé", value=(st.session_state.get("emp_ok_name","") if role=="موظف" else ""))

    if kind=="Revenus":
        r1,r2,r3 = st.columns(3)
        prix     = r1.number_input("💰 Prix (سعر التكوين)", min_value=0.0, step=10.0)
        m_admin  = r2.number_input("🏢 Montant Admin", min_value=0.0, step=10.0)
        m_struct = r3.number_input("🏫 Montant Structure", min_value=0.0, step=10.0)
        r4,r5 = st.columns(2)
        m_preins = r4.number_input("📝 Montant Pré-Inscription", min_value=0.0, step=10.0)
        ech      = r5.date_input("⏰ تاريخ الاستحقاق", value=date.today())
        e1,e2 = st.columns(2)
        mode  = e1.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
        cat   = e2.text_input("Catégorie", value="Revenus")
        note  = st.text_area("Note", placeholder="مثال: ClientTel:216XXXXXXXX")
        m_total = float(m_admin) + float(m_struct)

        # احتساب Reste داخل نفس الشهر لنفس libelle
        cur = fin_read_df(title, "Revenus")
        paid_so_far = 0.0
        if not cur.empty and "Libellé" in cur and "Montant_Total" in cur:
            same = cur[cur["Libellé"].fillna("").str.strip().str.lower() == libelle.strip().lower()]
            paid_so_far = float(same["Montant_Total"].sum()) if not same.empty else 0.0
        reste_after = max(float(prix) - (paid_so_far + float(m_total)), 0.0)
        st.caption(f"Total=(Admin+Structure): {m_total:.2f} — مدفوع سابقًا هذا الشهر: {paid_so_far:.2f} — Reste بعد الحفظ: {reste_after:.2f} — Pré-Inscr: {m_preins:.2f}")

        submit = st.form_submit_button("✅ حفظ العملية")
        if submit:
            if not libelle.strip():
                st.error("Libellé مطلوب.")
            elif prix <= 0:
                st.error("Prix مطلوب (>0).")
            elif m_total<=0 and m_preins<=0:
                st.error("المبلغ لازم > 0.")
            else:
                fin_append_row(title, {
                    "Date": fmt_date(date_val),
                    "Libellé": libelle.strip(),
                    "Prix": f"{float(prix):.2f}",
                    "Montant_Admin": f"{float(m_admin):.2f}",
                    "Montant_Structure": f"{float(m_struct):.2f}",
                    "Montant_PreInscription": f"{float(m_preins):.2f}",
                    "Montant_Total": f"{float(m_total):.2f}",
                    "Echeance": fmt_date(ech),
                    "Reste": f"{float(reste_after):.2f}",
                    "Mode": mode,
                    "Employé": employe.strip(),
                    "Catégorie": cat.strip(),
                    "Note": note.strip(),
                }, "Revenus")
                st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()
    else:
        r1,r2,r3 = st.columns(3)
        montant = r1.number_input("Montant", min_value=0.0, step=10.0)
        caisse  = r2.selectbox("Caisse_Source", ["Caisse_Admin","Caisse_Structure","Caisse_Inscription"])
        mode    = r3.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
        c2,c3 = st.columns(2)
        cat  = c2.text_input("Catégorie", value="Achat")
        note = c3.text_area("Note (اختياري)")

        submit = st.form_submit_button("✅ حفظ العملية")
        if submit:
            if not libelle.strip():
                st.error("Libellé مطلوب.")
            elif montant<=0:
                st.error("المبلغ لازم > 0.")
            else:
                fin_append_row(title, {
                    "Date": fmt_date(date_val),
                    "Libellé": libelle.strip(),
                    "Montant": f"{float(montant):.2f}",
                    "Caisse_Source": caisse,
                    "Mode": mode,
                    "Employé": employe.strip(),
                    "Catégorie": cat.strip(),
                    "Note": note.strip(),
                }, "Dépenses")
                st.success("تمّ الحفظ ✅"); st.cache_data.clear(); st.rerun()
