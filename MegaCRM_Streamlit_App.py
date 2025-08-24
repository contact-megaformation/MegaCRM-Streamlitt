# MegaCRM_Streamlit_App.py
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date
import urllib.parse
import webbrowser

SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS = Credentials.from_service_account_file("credentials.json", scopes=SCOPE)
client = gspread.authorize(CREDS)
SPREADSHEET_ID = "1DV0KyDRYHofWR60zdx63a9BWBywTFhLavGAExPIa6LI"

# تحميل كل البيانات
@st.cache_data(ttl=60)
def load_all_data():
    sh = client.open_by_key(SPREADSHEET_ID)
    all_data = []
    all_sheets = [ws.title for ws in sh.worksheets()]
    for sheet in all_sheets:
        try:
            ws = sh.worksheet(sheet)
            data = ws.get_all_records()
            df = pd.DataFrame(data)
            if df.empty:
                df = pd.DataFrame(columns=["Nom & Prénom", "Téléphone", "Type de contact", "Formation", "Remarque", "Date ajout", "Date de suivi", "Alerte", "Inscription", "Employe", "Tag"])
            df["Employe"] = sheet
            all_data.append(df)
        except:
            continue
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(), all_sheets

# إعداد الواجهة
st.set_page_config(layout="wide")
st.title("📊 MegaCRM - إدارة العملاء")

df_all, all_employes = load_all_data()

if df_all.empty:
    st.warning("⚠️ لا يوجد أي عملاء بعد. يمكنك إضافة أول عميل من الأسفل.")
else:
    df_all["Date ajout"] = pd.to_datetime(df_all["Date ajout"], format="%d/%m/%Y", errors="coerce")
    df_all["Mois"] = df_all["Date ajout"].dt.strftime("%m-%Y")

    # فلترة جانبية
    st.sidebar.header("🎛️ فلترة")
    emp_choice = st.sidebar.selectbox("👤 الموظف", ["الكل"] + all_employes)
    selected_month = st.sidebar.selectbox("📅 الشهر", ["الكل"] + sorted(df_all["Mois"].dropna().unique()))
    show_alert_only = st.sidebar.checkbox("🚨 عرض التنبيهات فقط")

    filtered = df_all.copy()
    if emp_choice != "الكل":
        filtered = filtered[filtered["Employe"] == emp_choice]
    if selected_month != "الكل":
        filtered = filtered[filtered["Mois"] == selected_month]
    if show_alert_only:
        filtered = filtered[filtered["Alerte"] == "🔴"]

    # تحديث Alerte
    today_str = date.today().strftime("%d/%m/%Y")
    for i, row in df_all.iterrows():
        if str(row["Date de suivi"]).strip() == today_str:
            try:
                ws = client.open_by_key(SPREADSHEET_ID).worksheet(row["Employe"])
                cell = ws.find(str(row["Téléphone"]))
                ws.update_cell(cell.row, 8, "🔴")
            except:
                continue

    # عرض العملاء
    st.subheader("📋 قائمة العملاء")
    if filtered.empty:
        st.info("لا توجد نتائج.")
    else:
        for i, row in filtered.iterrows():
            with st.expander(f"{row['Nom & Prénom']} - {row['Téléphone']}"):
                st.markdown(f"""
                **📚 Formation**: {row['Formation']}  
                **📞 Contact**: {row['Type de contact']}  
                **🗒️ Remarque**: {row['Remarque']}  
                **📆 Date ajout**: {row['Date ajout'].strftime('%d/%m/%Y') if pd.notna(row['Date ajout']) else ''}  
                **📅 Suivi**: {row['Date de suivi']}  
                **🚨 Alerte**: {"🔴" if row['Alerte'] == "🔴" else ""}  
                **✅ Inscription**: {row['Inscription']}  
                **🎨 Tag**: {row['Tag']}  
                """)

                # ملاحظة جديدة
                note = st.text_input("📝 ملاحظة جديدة", key=f"note_{i}")
                if st.button("📌 أضف الملاحظة", key=f"add_note_{i}") and note.strip():
                    try:
                        ws = client.open_by_key(SPREADSHEET_ID).worksheet(row["Employe"])
                        cell = ws.find(str(row["Téléphone"]))
                        old = ws.cell(cell.row, 5).value or ""
                        now = datetime.now().strftime("%d/%m/%Y %H:%M")
                        new_remarque = f"{old}\n⏱️ {now}: {note.strip()}"
                        ws.update_cell(cell.row, 5, new_remarque)
                        st.success("✅ تم حفظ الملاحظة")
                    except:
                        st.error("❌ خطأ أثناء حفظ الملاحظة")

                # تلوين العميل
                color = st.selectbox("🎯 تاغ العميل", ["", "متابعة", "مهتم", "مسجل"], key=f"tag_{i}")
                if st.button("🎨 حفظ التاغ", key=f"tag_btn_{i}") and color:
                    try:
                        ws = client.open_by_key(SPREADSHEET_ID).worksheet(row["Employe"])
                        cell = ws.find(str(row["Téléphone"]))
                        ws.update_cell(cell.row, 11, color)
                        ws.update_cell(cell.row, 9, color)
                        st.success("✅ تم حفظ التاغ")
                    except:
                        st.error("❌ خطأ أثناء حفظ التاغ")

                # WhatsApp
                if st.button("📲 إرسال واتساب", key=f"whatsapp_{i}"):
                    msg = urllib.parse.quote(f"Bonjour {row['Nom & Prénom']}, c'est MegaFormation. Suivi de votre formation.")
                    link = f"https://wa.me/{row['Téléphone']}?text={msg}"
                    webbrowser.open_new_tab(link)

# إضافة عميل جديد
st.subheader("➕ إضافة عميل جديد")
with st.form("add_client"):
    c1, c2 = st.columns(2)
    with c1:
        nom = st.text_input("👤 Nom & Prénom")
        tel = st.text_input("📞 Téléphone")
        formation = st.text_input("📚 Formation")
        contact_type = st.selectbox("Type de contact", ["Visiteur", "Appel téléphonique", "WhatsApp", "Social media"])
    with c2:
        employe = st.selectbox("👨‍💼 الموظف", all_employes)
        suivi = st.date_input("📅 Date de suivi")
        today = date.today().strftime("%d/%m/%Y")
    sub = st.form_submit_button("🆕 أضف العميل")
    if sub:
        if not nom or not tel or not formation:
            st.error("❌ الرجاء ملء كل الحقول")
        elif df_all["Téléphone"].astype(str).str.contains(tel).any():
            st.error("⚠️ العميل موجود مسبقًا بنفس رقم الهاتف")
        else:
            ws = client.open_by_key(SPREADSHEET_ID).worksheet(employe)
            ws.append_row([nom, tel, contact_type, formation, "", today, suivi.strftime("%d/%m/%Y"), "", "", employe, ""])
            st.success("✅ تمت إضافة العميل")

# إضافة/حذف موظف
st.subheader("👥 إدارة الموظفين")
with st.expander("➕ إضافة موظف جديد"):
    new_emp = st.text_input("🔤 اسم الموظف الجديد")
    if st.button("✅ إنشاء ورقة جديدة"):
        try:
            client.open_by_key(SPREADSHEET_ID).add_worksheet(title=new_emp, rows="100", cols="11")
            ws_new = client.open_by_key(SPREADSHEET_ID).worksheet(new_emp)
            ws_new.append_row(["Nom & Prénom", "Téléphone", "Type de contact", "Formation", "Remarque", "Date ajout", "Date de suivi", "Alerte", "Inscription", "Employe", "Tag"])
            st.success("✅ تمت إضافة الموظف")
        except:
            st.error("❌ خطأ في إنشاء الموظف")
