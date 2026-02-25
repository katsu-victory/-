import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import re
from datetime import datetime, timezone, timedelta
import email.utils
from urllib.parse import urljoin

# =========================
# 基本設定
# =========================

JST = timezone(timedelta(hours=+9), 'JST')
TODAY = datetime.now(JST).strftime("%Y-%m-%d")

REPORT_FILE = "update_report.csv"
HISTORY_FILE = "history.json"

KEYWORDS = ["ガイドライン", "指針", "診療手引き", "診療指針", "治療指針", "作成指針"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
}

# =========================
# 監視対象
# =========================

TARGETS = [
    {"name": "医学図書出版", "url": "https://igakutosho.co.jp/collections/book", "selector": "div.grid-view-item, .product-card", "type": "html"},
    {"name": "メディカルレビュー社", "url": "https://med.m-review.co.jp/merebo/products/book", "selector": ".product_list_item, li", "type": "html"},
    {"name": "診断と治療社", "url": "https://www.shindan.co.jp/", "selector": "dl, dt, li", "type": "html"},
    {"name": "南江堂", "url": "https://www.nankodo.co.jp/shinkan/list.aspx?div=d", "selector": "tr, div.shinkan-item", "type": "html"},
    {"name": "医学書院", "url": "https://www.igaku-shoin.co.jp/", "selector": "div.book-item, li", "type": "html"},
    {"name": "金原出版(GL検索)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=02", "selector": "div.book_list_item, tr, li", "type": "html"},
    {"name": "金原出版(規約検索)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=01", "selector": "div.book_list_item, tr, li", "type": "html"},
    {"name": "金原出版(規約PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/ky_new.pdf", "type": "pdf"},
    {"name": "金原出版(GL PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/gl_new.pdf", "type": "pdf"},
    {"name": "日本婦人科腫瘍学会", "url": "https://jsgo.or.jp/guideline/", "selector": "a", "type": "html"},
    {"name": "日本肺癌学会", "url": "https://www.haigan.gr.jp/publication/guideline/examination/2025/", "selector": "a", "type": "html"},
    {"name": "日本泌尿器科学会", "url": "https://www.urol.or.jp/other/guideline/", "selector": "a[href$='.pdf']", "type": "html_pdf_index"},
    {"name": "日本乳癌学会", "url": "https://jbcs.xsrv.jp/guideline/2022/", "selector": "a", "type": "html"},
    {"name": "日本頭頸部癌学会", "url": "http://www.jshnc.umin.ne.jp/guideline.html", "selector": "a", "type": "html"},
    {"name": "日本肝臓学会", "url": "https://www.jsh.or.jp/medical/guidelines/jsh_guidlines/medical/", "selector": "a", "type": "html"},
]

# =========================
# ユーティリティ
# =========================

def extract_date(text: str) -> str:
    patterns = [
        r'(\d{4}[./年]\d{1,2}[./月]\d{1,2})',
        r'(\d{4}[./年]\d{1,2})',
        r'(\d{4})(?:年版|版)'
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            nums = re.findall(r'\d+', m.group(1))
            if 2000 <= int(nums[0]) <= 2100:
                y = nums[0]
                mth = nums[1] if len(nums) > 1 else "01"
                d = nums[2] if len(nums) > 2 else "01"
                return f"{y}/{mth.zfill(2)}/{d.zfill(2)}"
    return "-"

def clean_title(text: str) -> str:
    text = re.sub(r'ISBN.*', '', text)
    text = re.sub(r'(定価|本体|税込).*', '', text)
    return " ".join(text.split()).strip()

# =========================
# サイトチェック
# =========================

def check_site(target):
    rows = []

    try:
        res = requests.get(target["url"], headers=HEADERS, timeout=30)
        res.raise_for_status()

        if target["type"] == "pdf":
            last = res.headers.get("Last-Modified", "")
            date = "-"
            if last:
                dt = email.utils.parsedate_to_datetime(last).astimezone(JST)
                date = dt.strftime("%Y/%m/%d")
            rows.append({
                "出版社": target["name"],
                "タイトル内容": f"【PDF更新監視】{target['name']}",
                "発刊日": date,
                "URL": target["url"]
            })
            return rows

        soup = BeautifulSoup(res.content, "html.parser")

        if target["type"] == "html_pdf_index":
            for a in soup.select(target["selector"]):
                title = a.get_text(strip=True)
                if any(k in title for k in KEYWORDS):
                    href = urljoin(target["url"], a.get("href"))
                    rows.append({
                        "出版社": target["name"],
                        "タイトル内容": clean_title(title),
                        "発刊日": extract_date(title),
                        "URL": href
                    })
            return rows

        for a in soup.select(target.get("selector", "a")):
            text = a.get_text(strip=True)
            if any(k in text for k in KEYWORDS) and 8 < len(text) < 300:
                rows.append({
                    "出版社": target["name"],
                    "タイトル内容": clean_title(text),
                    "発刊日": extract_date(text),
                    "URL": target["url"]
                })

    except Exception as e:
        print(f"[ERROR] {target['name']}: {e}")

    return rows

# =========================
# メイン処理
# =========================

def main():
    all_items = []

    for t in TARGETS:
        print(f"Checking: {t['name']}")
        all_items.extend(check_site(t))

    # 既存CSV読み込み
    if os.path.exists(REPORT_FILE):
        old = pd.read_csv(REPORT_FILE)
    else:
        old = pd.DataFrame(columns=["出版社","タイトル内容","発刊日","URL","ステータス","初回検知日","最終確認日"])

    known = set(old["タイトル内容"].astype(str))

    rows = []
    for item in all_items:
        title = item["タイトル内容"]

        if title in known:
            prev = old[old["タイトル内容"] == title].iloc[0]
            status = "既知"
            first = prev["初回検知日"]
        else:
            status = "★新着"
            first = TODAY

        rows.append({
            **item,
            "ステータス": status,
            "初回検知日": first,
            "最終確認日": TODAY
        })

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["タイトル内容"], keep="first")
    df = df.sort_values(["ステータス","出版社"], ascending=[True,True])

    df.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")
    print(f"Saved: {REPORT_FILE} ({len(df)} rows)")

if __name__ == "__main__":
    main()
