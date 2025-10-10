# Finance_App.py
# Finance-only (Revenus/Dépenses) — MB/Bizerte
# - Admin/Employee passwords (from secrets)
# - Client link shows previous paid + last reste + auto reste calc
# - Admin monthly & daily summaries
# - Month prev/next buttons, filters
# - Duplicate columns fix

import json, time, urllib.parse
import streamlit as st
import pandas as pd
import gspread
import gspread.exceptions as gse
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta

# ============ Page ============
st.set_page_config(page_title="Finance — MegaCRM", layout="wide")
st.markdown("<h1 style='text-align:center'>💸 Finance — Revenus / Dépenses (MB & Bizerte)</h1><hr/>", unsafe_allow_html=True)

# ============ Google Auth ============
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

# ============ Constants ============
FIN_MONTHS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Décembre"]
FIN_REV_COLUMNS = [
    "Date","Libellé","Prix",
    "Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total",
    "Echeance","Reste","Mode","Employé","Catégorie","Note"
]
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"]

EXPECTED_HEADERS_CLIENTS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

REASSIGN_LOG_SHEET   = "Reassign_Log"
REASSIGN_LOG_HEADERS = ["timestamp","moved_by","src_employee","dst_employee","client_name","phone"]

# ============ Helpers ============
def fmt_date(d: date|None) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else ""

def normalize_tn_phone(s: str) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if digits.startswith("216"): return digits
    if len(digits) == 8: return "216" + digits
    return digits

def fin_month_title(mois: str, kind: str, branch: str) -> str:
    prefix = "Revenue " if kind == "Revenus" else "Dépense "
    short  = "MB" if "Menzel" in branch else "BZ"
    return f"{prefix}{mois} ({short})"

def _to_num(s):
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

def _admin_password():
    return str(st.secrets.get("admin_password","admin123"))

def emp_pwd_for(emp_name:str)->str:
    try:
        mp = st.secrets["employee_passwords"]
        return str(mp.get(emp_name, mp.get("_default","1234")))
    except Exception:
        return "1234"

# ============ Locks ============
def admin_unlocked() -> bool:
    ok = st.session_state.get("admin_ok", False)
    ts = st.session_state.get("admin_ok_at")
    return bool(ok and ts and (datetime.now()-ts)<=timedelta(minutes=30))

def admin_lock_ui():
    with st.sidebar.expander("🔐 إدارة (Admin)", expanded=(not admin_unlocked())):
        if admin_unlocked():
            if st.button("قفل صفحة الأدمِن"):
                st.session_state["admin_ok"]=False; st.session_state["admin_ok_at"]=None; st.rerun()
        else:
            admin_pwd = st.text_input("كلمة سرّ الأدمِن", type="password")
            if st.button("فتح صفحة الأدمِن"):
                if admin_pwd == _admin_password():
                    st.session_state["admin_ok"]=True; st.session_state["admin_ok_at"]=datetime.now()
                    st.success("تمّ فتح صفحة الأدمِن (30 دقيقة).")
                else:
                    st.error("كلمة سرّ غير صحيحة.")

def emp_unlocked(emp_name:str)->bool:
    ok = st.session_state.get(f"emp_ok::{emp_name}", False)
    ts = st.session_state.get(f"emp_ok_at::{emp_name}")
    return bool(ok and ts and (datetime.now()-ts)<=timedelta(minutes=15))

def emp_lock_ui(emp_name: str):
    with st.sidebar.expander(f"🔐 فتح ورقة الموظّف: {emp_name}", expanded=not emp_unlocked(emp_name)):
        if emp_unlocked(emp_name):
            if st.button("قفل الآن", key=f"btn_close::{emp_name}"):
                st.session_state[f"emp_ok::{emp_name}"] = False
                st.session_state[f"emp_ok_at::{emp_name}"] = None
        else:
            pwd_try = st.text_input("كلمة سرّ الموظّف", type="password", key=f"pwd::{emp_name}")
            if st.button("فتح", key=f"btn_open::{emp_name}"):
                if pwd_try == emp_pwd_for(emp_name):
                    st.session_state[f"emp_ok::{emp_name}"] = True
                    st.session_state[f"emp_ok_at::{emp_name}"] = datetime.now()
                    st.success("تم الفتح لمدة 15 دقيقة.")
                else:
                    st.error("كلمة سرّ غير صحيحة.")

# ============ Sheets Utils ============
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
    st.error("تعذّر فتح Google Sheet (قد تكون الكوتا تجاوزت الحد).")
    raise last_err

def ensure_ws(title: str, columns: list[str]):
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 12)))
        ws.update("1:1", [columns + [""]*10])
        return ws
    header = ws.row_values(1)
    if header[:len(columns)] != columns or len(header) != len(columns):
        ws.update("1:1", [columns + [""]*10])
    return ws

@st.cache_data(ttl=120, show_spinner=False)
def _read_ws_all_values_cached(title: str, cols: tuple) -> list[list[str]]:
    ws = ensure_ws(title, list(cols))
    return ws.get_all_values()

def fin_read_df(title: str, kind: str) -> pd.DataFrame:
    expected = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    values = _read_ws_all_values_cached(title, tuple(expected))
    if not values:
        return pd.DataFrame(columns=expected)
    df = pd.DataFrame(values[1:], columns=values[0] if values else expected)
    df = df.loc[:, ~df.columns.duplicated()]
    for col in expected:
        if col not in df.columns:
            df[col] = ""
    df = df[expected]
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    if kind == "Revenus" and "Echeance" in df.columns:
        df["Echeance"] = pd.to_datetime(df["Echeance"], errors="coerce", dayfirst=True)
    if kind == "Revenus":
        for c in ["Prix","Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total","Reste"]:
            df[c] = _to_num(df[c])
        if "Alert" not in df.columns:
            df["Alert"] = ""
        if "Echeance" in df.columns and "Reste" in df.columns:
            today_ts = pd.Timestamp.now().normalize()
            ech = pd.to_datetime(df["Echeance"], errors="coerce")
            reste = pd.to_numeric(df["Reste"], errors="coerce").fillna(0.0)
            df.loc[ech.notna() & (ech < today_ts) & (reste > 0), "Alert"] = "⚠️ متأخر"
            df.loc[ech.notna() & (ech.dt.normalize() == today_ts) & (reste > 0), "Alert"] = "⏰ اليوم"
    else:
        df["Montant"] = _to_num(df["Montant"])
    return df

def fin_append_row(title: str, row: dict, kind: str):
    cols = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    ws = ensure_ws(title, cols)
    header = ws.row_values(1)
    vals = [str(row.get(col, "")) for col in header]
    ws.append_row(vals)
    _read_ws_all_values_cached.clear()

# ============ Load Clients/Employees ============
@st.cache_data(ttl=600)
def load_all_clients():
    sh = get_spreadsheet()
    dfs, employees = [], []
    for ws in sh.worksheets():
        t = ws.title.strip()
        if t.startswith("Revenue ") or t.startswith("Dépense "): continue
        if t.endswith("_PAIEMENTS") or t.startswith("_"): continue
        if t in (REASSIGN_LOG_SHEET,): continue
        rows = ws.get_all_values()
        if not rows: 
            continue
        header = rows[0]
        if header[:len(EXPECTED_HEADERS_CLIENTS)] != EXPECTED_HEADERS_CLIENTS:
            continue
        employees.append(t)
        df = pd.DataFrame(rows[1:], columns=header)
        df["__sheet_name"] = t
        dfs.append(df)
    big = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=EXPECTED_HEADERS_CLIENTS+["__sheet_name"])
    return big, employees

df_clients, all_employes = load_all_clients()

# ============ Sidebar: role/employee/branch/month ============
role = st.sidebar.radio("الدور", ["موظف","أدمن"], horizontal=True, index=0)

employee = st.sidebar.selectbox("👨‍💼 الموظّف", all_employes) if (role=="موظف" and all_employes) else ""
if role == "موظف" and employee:
    emp_lock_ui(employee)
    if not emp_unlocked(employee):
        st.info("🔒 أدخل كلمة سرّ الموظّف من اليسار لفتح اللوحة.")
        st.stop()

if role == "أدمن":
    admin_lock_ui()

branch  = st.sidebar.selectbox("🏢 الفرع", ["Menzel Bourguiba","Bizerte"])
kind_ar = st.sidebar.radio("النوع", ["مداخيل","مصاريف"], horizontal=True)
kind    = "Revenus" if kind_ar=="مداخيل" else "Dépenses"

# شهر مع أزرار ◀︎ ▶︎
mois_idx_default = datetime.now().month - 1
if "mois_idx" not in st.session_state:
    st.session_state["mois_idx"] = mois_idx_default
c_prev, c_sel, c_next = st.columns([1,3,1])
with c_prev:
    if st.button("◀︎ الشهر السابق"):
        st.session_state["mois_idx"] = (st.session_state["mois_idx"] - 1) % 12
with c_next:
    if st.button("▶︎ الشهر الموالي"):
        st.session_state["mois_idx"] = (st.session_state["mois_idx"] + 1) % 12
with c_sel:
    mois = st.selectbox("الشهر", FIN_MONTHS_FR, index=st.session_state["mois_idx"])

# حماية الفرع
BRANCH_PASSWORDS = _branch_passwords()
key_pw = f"finance_pw_ok::{branch}"
if key_pw not in st.session_state:
    st.session_state[key_pw] = False
if not st.session_state[key_pw]:
    with st.sidebar.expander("🔐 حماية الفرع"):
        pw_try = st.text_input("كلمة سرّ الفرع", type="password")
        if st.button("دخول"):
            if pw_try == BRANCH_PASSWORDS.get(branch,""):
                st.session_state[key_pw] = True
                st.success("تم الدخول ✅")
            else:
                st.error("كلمة سرّ غير صحيحة ❌")

if not st.session_state.get(key_pw, False):
    st.info("⬅️ أدخل كلمة السرّ من اليسار للمتابعة.")
    st.stop()

# ============ Read current month ============
fin_title = fin_month_title(mois, kind, branch)
df_fin = fin_read_df(fin_title, kind)
df_view = df_fin.copy()

# فلترة حسب الموظّف لو الدور "موظف"
if role == "موظف" and employee and "Employé" in df_view.columns:
    df_view = df_view[df_view["Employé"].fillna("").str.strip().str.lower() == employee.strip().lower()]

# ============ Filters ============
with st.expander("🔎 فلاتر"):
    c1, c2, c3 = st.columns(3)
    date_from = c1.date_input("من تاريخ", value=None)
    date_to   = c2.date_input("إلى تاريخ", value=None)
    search    = c3.text_input("بحث (Libellé/Catégorie/Mode/Note)")
    if "Date" in df_view.columns:
        if date_from: df_view = df_view[df_view["Date"] >= pd.to_datetime(date_from)]
        if date_to:   df_view = df_view[df_view["Date"] <= pd.to_datetime(date_to)]
    if search and not df_view.empty:
        m = pd.Series([False]*len(df_view))
        cols_search = ["Libellé","Catégorie","Mode","Employé","Note","Caisse_Source","Montant_PreInscription"]
        cols_search = [c for c in cols_search if c in df_view.columns]
        for c in cols_search:
            m |= df_view[c].fillna("").astype(str).str.contains(search, case=False, na=False)
        df_view = df_view[m]

# ============ Display ============
st.subheader(f"📄 {fin_title}")
if kind=="Revenus":
    cols_show = [c for c in ["Date","Libellé","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription",
                             "Montant_Total","Echeance","Reste","Alert","Mode","Employé","Catégorie","Note"] if c in df_view.columns]
else:
    cols_show = [c for c in ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"] if c in df_view.columns]

st.dataframe(df_view[cols_show] if not df_view.empty else pd.DataFrame(columns=cols_show), use_container_width=True)

# ============ Admin Summaries ============
if role == "أدمن" and admin_unlocked():
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
        else: dep_admin=dep_struct=dep_inscr=0.0
        reste_admin  = float(sum_admin)  - float(dep_admin)
        reste_struct = float(sum_struct) - float(dep_struct)
        reste_inscr  = float(sum_preins) - float(dep_inscr)
        a1,a2,a3 = st.columns(3)
        a1.metric("مداخيل Admin", f"{sum_admin:,.2f}")
        a2.metric("مصاريف Admin", f"{dep_admin:,.2f}")
        a3.metric("Reste Admin", f"{reste_admin:,.2f}")
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

    with st.expander("📆 ملخّص يومي — Admin/Structure (Admin Only)", expanded=False):
        rev_df = fin_read_df(fin_month_title(mois, "Revenus", branch), "Revenus")
        dep_df = fin_read_df(fin_month_title(mois, "Dépenses", branch), "Dépenses")
        for dcol in ("Date",):
            if dcol in rev_df.columns: rev_df[dcol] = pd.to_datetime(rev_df[dcol], errors="coerce")
            if dcol in dep_df.columns: dep_df[dcol] = pd.to_datetime(dep_df[dcol], errors="coerce")
        def _num(s): 
            return pd.to_numeric(pd.Series(s).astype(str).str.replace(" ","",regex=False).str.replace(",",".",regex=False), errors="coerce").fillna(0.0)
        if not rev_df.empty:
            if "Montant_Admin" in rev_df:  rev_df["Montant_Admin"]  = _num(rev_df["Montant_Admin"])
            if "Montant_Structure" in rev_df: rev_df["Montant_Structure"] = _num(rev_df["Montant_Structure"])
        if not dep_df.empty and "Montant" in dep_df:
            dep_df["Montant"] = _num(dep_df["Montant"])
        rev_day = pd.DataFrame(index=pd.to_datetime([]))
        if not rev_df.empty and "Date" in rev_df.columns:
            grp_rev = rev_df.groupby(rev_df["Date"].dt.normalize()).agg(
                Rev_Admin=("Montant_Admin", "sum"),
                Rev_Structure=("Montant_Structure", "sum"),
            )
            rev_day = grp_rev
        dep_day = pd.DataFrame(index=pd.to_datetime([]))
        if not dep_df.empty and "Date" in dep_df.columns and "Caisse_Source" in dep_df.columns:
            dep_admin_day = dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Admin"].groupby(dep_df["Date"].dt.normalize())["Montant"].sum().rename("Dep_Admin")
            dep_struct_day= dep_df.loc[dep_df["Caisse_Source"]=="Caisse_Structure"].groupby(dep_df["Date"].dt.normalize())["Montant"].sum().rename("Dep_Structure")
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
        cols_order = ["Date","Rev_Admin","Dep_Admin","Reste_Admin_Journalier","Reste_Admin_Cumulé",
                      "Rev_Structure","Dep_Structure","Reste_Structure_Journalier","Reste_Structure_Cumulé"]
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
        st.download_button("⬇️ تنزيل CSV (اليومي Admin/Structure)", data=csv_bytes,
                           file_name=f"daily_summary_{branch}_{mois}.csv", mime="text/csv")

# ============ Add New Operation ============
st.markdown("---")
st.subheader("➕ إضافة عملية جديدة")

client_default_lib, client_default_emp = "", (employee or "")
selected_client_info = None
paid_so_far_all, last_reste_all = 0.0, 0.0  # عبر كل الأشهر لنفس العميل/الفرع (Revenus)

if kind == "Revenus":
    st.markdown("#### 👤 اربط الدفعة بعميل مُسجَّل (اختياري)")
    reg = df_clients.copy()
    reg["Inscr_norm"] = reg["Inscription"].fillna("").astype(str).str.lower().str.strip()
    reg = reg[reg["Inscr_norm"].isin(["oui","inscrit"])]
    if role == "موظف" and employee:
        reg = reg[reg["__sheet_name"] == employee]

    options, pick = [], None
    if not reg.empty:
        def _opt(r):
            ph = normalize_tn_phone(r.get("Téléphone",""))
            return f"{r.get('Nom & Prénom','')} — +{ph} — {r.get('Formation','')}  [{r.get('__sheet_name','')}]"
        options = [_opt(r) for _, r in reg.iterrows()]
        pick = st.selectbox("اختر عميلًا مُسجَّلًا", ["— بدون اختيار —"]+options)

    if pick and pick!="— بدون اختيار —":
        row = reg.iloc[options.index(pick)]
        selected_client_info = {
            "name": str(row.get("Nom & Prénom","")).strip(),
            "tel":  normalize_tn_phone(str(row.get("Téléphone","")).strip()),
            "formation": str(row.get("Formation","")).strip(),
            "emp": str(row.get("__sheet_name","")).strip()
        }
        client_default_lib = f"Paiement {selected_client_info['formation']} - {selected_client_info['name']}"
        if not client_default_emp:
            client_default_emp = selected_client_info["emp"]

        # دفعات سابقة لنفس الفرع عبر كل الأشهر
        out = []
        try:
            sh_titles = [w.title for w in get_spreadsheet().worksheets()]
        except Exception:
            sh_titles = []
        months_available = [m for m in FIN_MONTHS_FR if fin_month_title(m, "Revenus", branch) in sh_titles]
        for m in months_available:
            t = fin_month_title(m, "Revenus", branch)
            try:
                dfm = fin_read_df(t, "Revenus")
            except Exception:
                dfm = pd.DataFrame(columns=FIN_REV_COLUMNS)
            if dfm.empty: 
                continue
            dfm = dfm.copy()
            note_series = dfm["Note"].astype(str) if "Note" in dfm.columns else pd.Series([""]*len(dfm))
            lib_series = (dfm["Libellé"].astype(str).str.strip().str.lower()
                          if "Libellé" in dfm.columns else pd.Series([""]*len(dfm)))
            cond_lib = lib_series.eq(client_default_lib.strip().lower())
            cond_phone = note_series.str.contains(selected_client_info["tel"], na=False, regex=False)
            sub = dfm[cond_lib | cond_phone].copy()
            if not sub.empty:
                sub["__mois"] = m
                sub["__sheet_title"] = t
                out.append(sub)
        prev_df = pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=FIN_REV_COLUMNS+["__sheet_title","__mois"])
        prev_df = prev_df.loc[:, ~prev_df.columns.duplicated()]
        st.markdown("#### 💾 دفعات سابقة (كل الأشهر لهذا الفرع)")
        if prev_df.empty:
            st.caption("لا توجد دفعات مسجّلة.")
        else:
            show_cols = ["__mois","Date","Prix","Montant_Admin","Montant_Structure","Montant_PreInscription",
                         "Montant_Total","Reste","Mode","Employé","Catégorie","Note"]
            show_cols = [c for c in show_cols if c in prev_df.columns]
            st.dataframe(prev_df[show_cols], use_container_width=True)
            paid_so_far_all = float(prev_df.get("Montant_Total", pd.Series(dtype=float)).sum())
            last_reste_all = float(prev_df.get("Reste", pd.Series(dtype=float)).fillna(0).iloc[-1])

        st.info(f"🔎 مجموع ما دُفع سابقًا: {paid_so_far_all:,.2f} — آخر Reste: {last_reste_all:,.2f}")

# === Form ===
with st.form("fin_add_row"):
    d1, d2, d3 = st.columns(3)
    date_val = d1.date_input("Date", value=datetime.today())
    libelle  = d2.text_input("Libellé", value=(client_default_lib if kind=="Revenus" else ""))
    employe  = d3.text_input("Employé", value=(client_default_emp if kind=="Revenus" else (employee or "")))

    if kind=="Revenus":
        r1, r2, r3 = st.columns(3)
        prix    = r1.number_input("💰 Prix (سعر التكوين)", min_value=0.0, step=10.0, value=0.0)
        m_admin = r2.number_input("🏢 Montant Admin",    min_value=0.0, step=10.0, value=0.0)
        m_str   = r3.number_input("🏫 Montant Structure", min_value=0.0, step=10.0, value=0.0)

        r4, r5 = st.columns(2)
        m_pre  = r4.number_input("📝 Montant Pré-Inscription", min_value=0.0, step=10.0, value=0.0)
        ech    = r5.date_input("⏰ تاريخ الاستحقاق", value=date.today())

        m_total = float(m_admin) + float(m_str)

        # مدفوع سابقًا في نفس الشهر لنفس libellé (للreste الحالي)
        cur = fin_read_df(fin_title, "Revenus")
        paid_so_far_month = 0.0
        if not cur.empty and "Libellé" in cur and "Montant_Total" in cur:
            same = cur[cur["Libellé"].fillna("").str.strip().str.lower() == libelle.strip().lower()]
            paid_so_far_month = float(same["Montant_Total"].sum()) if not same.empty else 0.0

        reste_after = max(float(prix) - (paid_so_far_month + float(m_total)), 0.0)

        e1, e2 = st.columns(2)
        mode  = e1.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
        cat   = e2.text_input("Catégorie", value="Revenus")
        note_default = f"ClientTel:{selected_client_info['tel']}" if selected_client_info else ""
        note = st.text_area("Note", value=note_default)

        st.caption(f"سابقًا (كل الأشهر): {paid_so_far_all:.2f} — آخر Reste: {last_reste_all:.2f}")
        st.caption(f"هذا الشهر — Total=(Admin+Structure): {m_total:.2f} — مدفوع سابقًا: {paid_so_far_month:.2f} — Reste بعد الحفظ: {reste_after:.2f} — Pré-Inscr: {m_pre:.2f}")

        submit_ok = st.form_submit_button("✅ حفظ العملية")
        if submit_ok:
            if not libelle.strip():
                st.error("Libellé مطلوب.")
            elif prix <= 0:
                st.error("Prix مطلوب (> 0).")
            elif m_total <= 0 and m_pre <= 0:
                st.error("المبلغ لازم > 0.")
            else:
                fin_append_row(fin_title, {
                    "Date": fmt_date(date_val),
                    "Libellé": libelle.strip(),
                    "Prix": f"{float(prix):.2f}",
                    "Montant_Admin": f"{float(m_admin):.2f}",
                    "Montant_Structure": f"{float(m_str):.2f}",
                    "Montant_PreInscription": f"{float(m_pre):.2f}",
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
        c1, c2, c3 = st.columns(3)
        montant = c1.number_input("Montant", min_value=0.0, step=10.0)
        caisse  = c2.selectbox("Caisse_Source", ["Caisse_Admin","Caisse_Structure","Caisse_Inscription"])
        mode    = c3.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
        c4, c5 = st.columns(2)
        cat  = c4.text_input("Catégorie", value="Achat")
        note = c5.text_area("Note (اختياري)")

        submit_ok = st.form_submit_button("✅ حفظ العملية")
        if submit_ok:
            if not libelle.strip():
                st.error("Libellé مطلوب.")
            elif montant <= 0:
                st.error("المبلغ لازم > 0.")
            else:
                fin_append_row(fin_title, {
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
