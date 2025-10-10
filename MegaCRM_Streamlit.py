# Finance_App.py
# Finance-only (Revenus/Dépenses) for MB/Bizerte
# - Clean headers (avoid duplicate columns)
# - Employee select + link payment to registered client
# - Month prev/next buttons
# - Streamlit + Google Sheets (gspread + service account)

import json, time, urllib.parse
import streamlit as st
import pandas as pd
import gspread
import gspread.exceptions as gse
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta

# =================== Page setup ===================
st.set_page_config(page_title="Finance — MegaCRM", layout="wide")
st.markdown(
    "<h1 style='text-align:center'>💸 Finance — Revenus / Dépenses (MB & Bizerte)</h1><hr/>",
    unsafe_allow_html=True
)

# =================== Google Auth ===================
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
        # fallback for local dev (put your JSON locally + sheet id)
        creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet_id = "PUT_YOUR_SHEET_ID_HERE"
        return client, sheet_id

client, SPREADSHEET_ID = make_client_and_sheet_id()

# =================== Constants ===================
FIN_MONTHS_FR = [
    "Janvier","Février","Mars","Avril","Mai","Juin",
    "Juillet","Aout","Septembre","Octobre","Novembre","Décembre"
]
FIN_REV_COLUMNS = [
    "Date","Libellé","Prix",
    "Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total",
    "Echeance","Reste",
    "Mode","Employé","Catégorie","Note"
]
FIN_DEP_COLUMNS = ["Date","Libellé","Montant","Caisse_Source","Mode","Employé","Catégorie","Note"]

EXPECTED_HEADERS_CLIENTS = [
    "Nom & Prénom","Téléphone","Type de contact","Formation",
    "Remarque","Date ajout","Date de suivi","Alerte",
    "Inscription","Employe","Tag"
]

def _branch_passwords():
    try:
        b = st.secrets["branch_passwords"]
        return {
            "Menzel Bourguiba": str(b.get("MB","MB_2025!")),
            "Bizerte": str(b.get("BZ","BZ_2025!"))
        }
    except Exception:
        return {"Menzel Bourguiba":"MB_2025!","Bizerte":"BZ_2025!"}

# =================== Helpers ===================
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

# =================== Sheets Utils (with backoff/cache) ===================
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
    """Ensure worksheet exists with a clean, exact header (wipes leftover header cells)."""
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(max(len(columns), 12)))
        ws.update("1:1", [columns + [""]*10])
        return ws

    header = ws.row_values(1)
    # If header is not an exact match in size or content, rewrite and pad to kill leftovers
    if header[:len(columns)] != columns or len(header) != len(columns):
        ws.update("1:1", [columns + [""]*10])
    return ws

@st.cache_data(ttl=120, show_spinner=False)
def _read_ws_all_values_cached(title: str, cols: tuple) -> list[list[str]]:
    ws = ensure_ws(title, list(cols))
    return ws.get_all_values()

def _to_num(s):
    return (
        pd.Series(s).astype(str)
        .str.replace(" ","",regex=False)
        .str.replace(",",".",regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )

def fin_read_df(title: str, kind: str) -> pd.DataFrame:
    """Read Revenus/Dépenses with duplicate-column cleanup and typed fields."""
    expected = FIN_REV_COLUMNS if kind == "Revenus" else FIN_DEP_COLUMNS
    values = _read_ws_all_values_cached(title, tuple(expected))
    if not values:
        return pd.DataFrame(columns=expected)

    df = pd.DataFrame(values[1:], columns=values[0] if values else expected)
    # Drop duplicate column names
    df = df.loc[:, ~df.columns.duplicated()]
    # Add any missing expected columns, then reorder strictly to expected
    for col in expected:
        if col not in df.columns:
            df[col] = ""
    df = df[expected]

    # Types
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

# =================== Load clients (for linking) ===================
@st.cache_data(ttl=600)
def load_all_clients():
    """Load all employee sheets that match the client header; return big DF + employee list."""
    sh = get_spreadsheet()
    dfs, employees = [], []
    for ws in sh.worksheets():
        t = ws.title.strip()
        if t.startswith("Revenue ") or t.startswith("Dépense "): continue
        if t.endswith("_PAIEMENTS") or t.startswith("_") or t in ("Reassign_Log",): continue
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

# =================== Sidebar (role/employee/branch/month) ===================
role = st.sidebar.radio("الدور", ["موظف","أدمن"], horizontal=True, index=0)
employee = st.sidebar.selectbox("👨‍💼 الموظّف", all_employes) if (role=="موظف" and all_employes) else ""

branch  = st.sidebar.selectbox("🏢 الفرع", ["Menzel Bourguiba","Bizerte"])
kind_ar = st.sidebar.radio("النوع", ["مداخيل","مصاريف"], horizontal=True)
kind    = "Revenus" if kind_ar=="مداخيل" else "Dépenses"

# month select + prev/next buttons
mois_idx_default = datetime.now().month - 1
if "mois_idx" not in st.session_state:
    st.session_state["mois_idx"] = mois_idx_default
c_btn1, c_sel, c_btn2 = st.columns([1,3,1])
with c_btn1:
    if st.button("◀︎ الشهر السابق"):
        st.session_state["mois_idx"] = (st.session_state["mois_idx"] - 1) % 12
with c_btn2:
    if st.button("▶︎ الشهر الموالي"):
        st.session_state["mois_idx"] = (st.session_state["mois_idx"] + 1) % 12
with c_sel:
    mois = st.selectbox("الشهر", FIN_MONTHS_FR, index=st.session_state["mois_idx"])

# branch-level password
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

# =================== Read current month sheet ===================
fin_title = fin_month_title(mois, kind, branch)
df_fin = fin_read_df(fin_title, kind)
df_view = df_fin.copy()

# Employee filter for employees
if role == "موظف" and employee and "Employé" in df_view.columns:
    df_view = df_view[df_view["Employé"].fillna("").str.strip().str.lower() == employee.strip().lower()]

# =================== Filters ===================
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

# =================== Display ===================
st.subheader(f"📄 {fin_title}")
if kind=="Revenus":
    cols_show = [c for c in [
        "Date","Libellé","Prix",
        "Montant_Admin","Montant_Structure","Montant_PreInscription","Montant_Total",
        "Echeance","Reste","Alert",
        "Mode","Employé","Catégorie","Note"
    ] if c in df_view.columns]
else:
    cols_show = [c for c in [
        "Date","Libellé","Montant","Caisse_Source",
        "Mode","Employé","Catégorie","Note"
    ] if c in df_view.columns]

st.dataframe(
    df_view[cols_show] if not df_view.empty else pd.DataFrame(columns=cols_show),
    use_container_width=True
)

# =================== Add Operation ===================
st.markdown("---")
st.subheader("➕ إضافة عملية جديدة")

# (A) Link to registered client (for Revenus)
client_default_lib, client_default_emp = "", (employee or "")
selected_client_info = None

if kind == "Revenus":
    st.markdown("#### 👤 اربط الدفعة بعميل مُسجَّل (اختياري)")
    reg = df_clients.copy()
    # registered only
    reg["Inscr_norm"] = reg["Inscription"].fillna("").astype(str).str.lower().str.strip()
    reg = reg[reg["Inscr_norm"].isin(["oui","inscrit"])]
    # if employee role -> limit to his sheet
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

# (B) The form
with st.form("fin_add_row"):
    d1, d2, d3 = st.columns(3)
    date_val = d1.date_input("Date", value=datetime.today())
    libelle  = d2.text_input("Libellé", value=(client_default_lib if kind=="Revenus" else ""))
    employe  = d3.text_input("Employé", value=(client_default_emp if kind=="Revenus" else (employee or "")))

    if kind=="Revenus":
        r1, r2, r3 = st.columns(3)
        prix    = r1.number_input("💰 Prix (سعر التكوين)", min_value=0.0, step=10.0)
        m_admin = r2.number_input("🏢 Montant Admin",    min_value=0.0, step=10.0)
        m_str   = r3.number_input("🏫 Montant Structure", min_value=0.0, step=10.0)

        r4, r5 = st.columns(2)
        m_pre  = r4.number_input("📝 Montant Pré-Inscription", min_value=0.0, step=10.0)
        ech    = r5.date_input("⏰ تاريخ الاستحقاق", value=date.today())

        m_total = float(m_admin) + float(m_str)

        # compute reste within same month+branch for same libellé
        cur = fin_read_df(fin_title, "Revenus")
        paid_so_far = 0.0
        if not cur.empty and "Libellé" in cur and "Montant_Total" in cur:
            same = cur[cur["Libellé"].fillna("").str.strip().str.lower() == libelle.strip().lower()]
            paid_so_far = float(same["Montant_Total"].sum()) if not same.empty else 0.0
        reste_after = max(float(prix) - (paid_so_far + float(m_total)), 0.0)

        e1, e2 = st.columns(2)
        mode  = e1.selectbox("Mode", ["Espèces","Virement","Carte","Chèque","Autre"])
        cat   = e2.text_input("Catégorie", value="Revenus")
        note_default = f"ClientTel:{selected_client_info['tel']}" if selected_client_info else ""
        note = st.text_area("Note", value=note_default)

        st.caption(f"Total=(Admin+Structure): {m_total:.2f} — مدفوع سابقًا هذا الشهر: {paid_so_far:.2f} — Reste بعد الحفظ: {reste_after:.2f} — Pré-Inscr: {m_pre:.2f}")

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
