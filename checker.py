import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
from datetime import datetime, timezone, timedelta
import email.utils
from urllib.parse import urljoin

# =========================
# 基本設定
# =========================

JST = timezone(timedelta(hours=+9))
TODAY = datetime.now(JST).strftime("%Y-%m-%d")
REPORT_FILE = "update_report.csv"

KEYWORDS = ["ガイドライン", "指針", "診療手引き", "診療指針", "治療指針", "取扱い規約"]
HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# 正規化
# =========================

def normalize_title(title):
    t = title.lower()
    t = re.sub(r'\d{4}', '', t)
    t = re.sub(r'(年版|改訂|版|ver\.?)', '', t)
    t = re.sub(r'(について|概要|解説|about)', '', t)
    t = re.sub(r'[^\wぁ-んァ-ン一-龥]', '', t)
    return t.strip()

def extract_year(text):
    if not text:
        return "-"
    m = re.search(r'(20\d{2})', text)
    return m.group(1) if m else "-"
def extract_publish_date(text=None, url=None):
    """
    優先順位:
    1) テキスト内の日付（YYYY/MM/DD, YYYY-MM-DD）
    2) YYYY年MM月DD日
    3) リンク先本文から取得
    4) Last-Modified
    5) 年のみ
    """

    if text:
        # 1) 2024/03/01 or 2024-03-01
        m = re.search(r'(20\d{2})[/-](\d{1,2})[/-](\d{1,2})', text)
        if m:
            return f"{m.group(1)}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"

        # 2) 2024年3月1日
        m = re.search(r'(20\d{2})年\s*(\d{1,2})月\s*(\d{1,2})日', text)
        if m:
            return f"{m.group(1)}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"

    # 3) リンク先本文を見る
    if url:
        try:
            page = requests.get(url, headers=HEADERS, timeout=15)
            soup2 = BeautifulSoup(page.content, "html.parser")
            body_text = soup2.get_text(" ", strip=True)

            m = re.search(r'(20\d{2})[/-](\d{1,2})[/-](\d{1,2})', body_text)
            if m:
                return f"{m.group(1)}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"

            m = re.search(r'(20\d{2})年\s*(\d{1,2})月\s*(\d{1,2})日', body_text)
            if m:
                return f"{m.group(1)}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"

        except:
            pass

        # 4) Last-Modified
        try:
            res = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
            lm = res.headers.get("Last-Modified")
            if lm:
                dt = email.utils.parsedate_to_datetime(lm).astimezone(JST)
                return dt.strftime("%Y/%m/%d")
        except:
            pass

    # 5) 年だけ
    if text:
        m = re.search(r'(20\d{2})', text)
        if m:
            return m.group(1)

    return "-"
def get_last_modified(url):
    try:
        res = requests.head(url, headers=HEADERS, timeout=20, allow_redirects=True)
        lm = res.headers.get("Last-Modified")
        if lm:
            dt = email.utils.parsedate_to_datetime(lm).astimezone(JST)
            return dt.strftime("%Y/%m/%d")
    except:
        pass
    return "-"
def extract_publish_date_strict(text=None, url=None):
    """
    発刊日（YYYY/MM/DD）が明示されている場合のみ返す
    推定・更新日は含めない
    """
    if text:
        m = re.search(r'(20\d{2})[/-](\d{1,2})[/-](\d{1,2})', text)
        if m:
            return f"{m.group(1)}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"

        m = re.search(r'(20\d{2})年\s*(\d{1,2})月\s*(\d{1,2})日', text)
        if m:
            return f"{m.group(1)}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"

    if url:
        try:
            page = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(page.content, "html.parser")
            body = soup.get_text(" ", strip=True)

            m = re.search(r'(20\d{2})[/-](\d{1,2})[/-](\d{1,2})', body)
            if m:
                return f"{m.group(1)}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"

            m = re.search(r'(20\d{2})年\s*(\d{1,2})月\s*(\d{1,2})日', body)
            if m:
                return f"{m.group(1)}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"
        except:
            pass

    return ""
def extract_last_modified(url):
    try:
        res = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        lm = res.headers.get("Last-Modified")
        if lm:
            dt = email.utils.parsedate_to_datetime(lm).astimezone(JST)
            return dt.strftime("%Y/%m/%d")
    except:
        pass
    return ""
def extract_revision_year(text):
    if not text:
        return ""
    m = re.search(r'(20\d{2})', text)
    return m.group(1) if m else ""
# =========================
# 監視対象
# =========================

TARGETS = [

    # ================= 出版社系 =================

    {
        "name": "医学図書出版",
        "publisher_key": "igakutosho",
        "url": "https://igakutosho.co.jp/collections/book",
        "selector": "div.grid-view-item, .product-card",
        "type": "html"
    },
    {
        "name": "メディカルレビュー社",
        "publisher_key": "medical_review",
        "url": "https://med.m-review.co.jp/merebo/products/book",
        "selector": ".product_list_item, li",
        "type": "html"
    },
    {
        "name": "診断と治療社",
        "publisher_key": "shindan",
        "url": "https://www.shindan.co.jp/",
        "selector": "dl, dt, li",
        "type": "html"
    },
    {
        "name": "南江堂",
        "publisher_key": "nankodo",
        "url": "https://www.nankodo.co.jp/shinkan/list.aspx?div=d",
        "selector": "tr, div.shinkan-item",
        "type": "html"
    },
    {
        "name": "医学書院",
        "publisher_key": "igakushoin",
        "url": "https://www.igaku-shoin.co.jp/",
        "selector": "div.book-item, li",
        "type": "html"
    },
    {
        "name": "金原出版(GL検索)",
        "publisher_key": "kanehara_gl",
        "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=02",
        "selector": "div.book_list_item, tr, li",
        "type": "html"
    },
    {
        "name": "金原出版(規約検索)",
        "publisher_key": "kanehara_rule",
        "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=01",
        "selector": "div.book_list_item, tr, li",
        "type": "html"
    },
    {
        "name": "金原出版(規約PDF)",
        "publisher_key": "kanehara_rule_pdf",
        "url": "https://www.kanehara-shuppan.co.jp/_data/books/ky_new.pdf",
        "type": "pdf"
    },
    {
        "name": "金原出版(GL PDF)",
        "publisher_key": "kanehara_gl_pdf",
        "url": "https://www.kanehara-shuppan.co.jp/_data/books/gl_new.pdf",
        "type": "pdf"
    },

    # ================= 学会系 =================

    {
        "name": "日本婦人科腫瘍学会",
        "publisher_key": "jsgo",
        "url": "https://jsgo.or.jp/guideline/",
        "selector": "a",
        "type": "html"
    },
    {
        "name": "日本肺癌学会",
        "publisher_key": "haigan",
        "url": "https://www.haigan.gr.jp/publication/guideline/examination/2025/",
        "selector": "a",
        "type": "html"
    },
    {
        "name": "日本泌尿器科学会",
        "publisher_key": "urol",
        "url": "https://www.urol.or.jp/other/guideline/",
        "selector": "a[href$='.pdf']",
        "type": "html_pdf_index"
    },
    {
        "name": "日本乳癌学会",
        "publisher_key": "jbcs",
        "url": "https://jbcs.xsrv.jp/guideline/2022/",
        "selector": "a",
        "type": "html"
    },
    {
        "name": "日本頭頸部癌学会",
        "publisher_key": "jshnc",
        "url": "http://www.jshnc.umin.ne.jp/guideline.html",
        "selector": "a",
        "type": "html"
    },
    {
        "name": "日本肝臓学会",
        "publisher_key": "jsh",
        "url": "https://www.jsh.or.jp/medical/guidelines/jsh_guidlines/medical/",
        "selector": "a",
        "type": "html"
    },
]

# =========================
# サイトチェック
# =========================

def check_site(target):
    rows = []

    try:
        # ---------- PDF単体 ----------
        if target["type"] == "pdf":
            res = requests.head(target["url"], headers=HEADERS, timeout=30, allow_redirects=True)
            last = res.headers.get("Last-Modified")
            date = "-"
            if last:
                dt = email.utils.parsedate_to_datetime(last).astimezone(JST)
                date = dt.strftime("%Y/%m/%d")

            rows.append({
                "論理ID": f"{target['publisher_key']}_pdf",
                "正式タイトル": target["name"],
                "出版社": target["name"],
                "種別": "PDF",
                "版情報": "-",
                "発刊日": date,
                "URL": target["url"]
            })
            return rows

        # ---------- HTML ----------
        res = requests.get(target["url"], headers=HEADERS, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")

        # ---------- PDFリンク一覧 ----------
        if target["type"] == "html_pdf_index":
            for a in soup.select(target["selector"]):
                title = a.get_text(strip=True)
                if any(k in title for k in KEYWORDS):
                    href = a.get("href")
                    url = urljoin(target["url"], href) if href else target["url"]
                    norm = normalize_title(title)

                    pub_date = extract_publish_date(title, url)

                    rows.append({
                        "論理ID": f"{target['publisher_key']}_{norm}",
                        "正式タイトル": title,
                        "出版社": target["name"],
                        "種別": "PDF",
                        "版情報": extract_year(title),
                        "発刊日（明示）": extract_publish_date_strict(text, url),
                        "改訂年": extract_revision_year(text),
                        "最終更新日": extract_last_modified(url),
                        "URL": url
                    })
            return rows

        # ---------- 通常HTML ----------
        for a in soup.select(target["selector"]):
            text = a.get_text(strip=True)
            if any(k in text for k in KEYWORDS) and 8 < len(text) < 200:
                href = a.get("href")
                url = urljoin(target["url"], href) if href else target["url"]
                norm = normalize_title(text)

                pub_date = extract_publish_date(text, url)

                rows.append({
                    "論理ID": f"{target['publisher_key']}_{norm}",
                    "正式タイトル": text,
                    "出版社": target["name"],
                    "種別": "Web",
                    "版情報": extract_year(text),
                    "発刊日（明示）": extract_publish_date_strict(text, url),
                    "改訂年": extract_revision_year(text),
                    "最終更新日": extract_last_modified(url),
                    "URL": url
                })

    except Exception as e:
        print(f"[ERROR] {target['name']} {e}")

    return rows

# =========================
# メイン（完全修正版）
# =========================

def main():
    print("=== Collecting current data ===")

    current_rows = []
    for t in TARGETS:
        print(f"Checking {t['name']}")
        current_rows.extend(check_site(t))

    current_df = pd.DataFrame(current_rows)

    if current_df.empty:
        print("No data collected today.")
        return

    # ---------- 旧CSV読み込み（安全版） ----------
    if os.path.exists(REPORT_FILE):
        old = pd.read_csv(REPORT_FILE)

        # 列不足対応（後方互換）
        required_cols = [
                    "論理ID",
                    "正式タイトル",
                    "出版社",
                    "種別",
                    "発刊日（明示）",
                    "改訂年",
                    "最終更新日",
                    "URL",
                    "ステータス",
                    "初回検知日",
                    "最終確認日"
                ]
        for col in required_cols:
            if col not in old.columns:
                old[col] = ""
    else:
        old = pd.DataFrame(columns=[
            "論理ID","正式タイトル","出版社","種別","版情報",
            "発刊日","URL","ステータス","初回検知日","最終確認日"
        ])

    # ---------- マスター統合 ----------
    old = old.set_index("論理ID", drop=False)
    current_df = current_df.set_index("論理ID", drop=False)

    merged = old.copy()

    for lid, row in current_df.iterrows():

        if lid in merged.index:
            # 既知
            merged.loc[lid, ["正式タイトル","出版社","種別","版情報","発刊日","URL"]] = \
                row[["正式タイトル","出版社","種別","版情報","発刊日","URL"]]
            merged.loc[lid, "ステータス"] = "既知"
            merged.loc[lid, "最終確認日"] = TODAY
        else:
            # 新規
            new_row = row.to_dict()
            new_row["ステータス"] = "★新着"
            new_row["初回検知日"] = TODAY
            new_row["最終確認日"] = TODAY
            merged.loc[lid] = new_row
            
        # ---------- 出力 ----------
        final_df = merged.reset_index(drop=True)
        
        # 重複整理（論理ID + URL）
        final_df = (
            final_df
            .sort_values(["論理ID", "URL"])
            .groupby("論理ID", as_index=False)
            .first()
        )
        
        # CSV更新日時（JST）
        final_df["CSV更新日時"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # 表示用ソート
        final_df = final_df.sort_values(["出版社", "論理ID"])
        
        final_df.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")
        
        print("Saved update_report.csv (FULL MASTER)")

if __name__ == "__main__":
    main()







