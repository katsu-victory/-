import requests
from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import re
from datetime import datetime, timezone, timedelta
import email.utils

# =========================
# タイムゾーン設定（JST）
# =========================
JST = timezone(timedelta(hours=+9), 'JST')

# =========================
# 監視対象設定
# =========================
TARGETS = [
    {"name": "医学図書出版", "url": "https://igakutosho.co.jp/collections/book",
     "selector": "div.grid-view-item, .product-card", "type": "html"},

    {"name": "メディカルレビュー社", "url": "https://med.m-review.co.jp/merebo/products/book",
     "selector": ".product_list_item, li", "type": "html"},

    {"name": "診断と治療社", "url": "https://www.shindan.co.jp/",
     "selector": "dl, dt, li", "type": "html"},

    {"name": "南江堂", "url": "https://www.nankodo.co.jp/shinkan/list.aspx?div=d",
     "selector": "tr, div.shinkan-item", "type": "html"},

    {"name": "医学書院", "url": "https://www.igaku-shoin.co.jp/",
     "selector": "div.book-item, li", "type": "html"},

    {"name": "金原出版(GL検索)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=02",
     "selector": "div.book_list_item, tr, li", "type": "html"},

    {"name": "金原出版(規約検索)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=01",
     "selector": "div.book_list_item, tr, li", "type": "html"},

    {"name": "金原出版(お知らせ)", "url": "https://www.kanehara-shuppan.co.jp/news/index.html?no=151",
     "selector": "dl > *, li", "type": "html"},

    {"name": "金原出版(規約PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/ky_new.pdf",
     "type": "pdf_header"},

    {"name": "金原出版(GL PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/gl_new.pdf",
     "type": "pdf_header"},

    {"name": "日本婦人科腫瘍学会_治療ガイドライン", "url": "https://jsgo.or.jp/guideline/",
     "selector": "li, dl, dt, a", "type": "html"},

    {"name": "日本肺癌学会_診療ガイドライン",
     "url_template": "https://www.haigan.gr.jp/publication/guideline/examination/{year}/",
     "version": {"year": "2025"},
     "selector": "li, dl, a",
     "type": "html_versioned"},

    {"name": "日本泌尿器科学会_ガイドライン",
     "url": "https://www.urol.or.jp/other/guideline/",
     "selector": "a[href$='.pdf']",
     "type": "html_pdf_index"},

    {"name": "日本乳癌学会_乳癌診療ガイドライン2022",
     "url": "https://jbcs.xsrv.jp/guideline/2022/",
     "selector": "li, dl, dt, a", "type": "html"},

    {"name": "日本頭頸部癌学会_ガイドライン",
     "url": "http://www.jshnc.umin.ne.jp/guideline.html",
     "selector": "li, dl, dt, a", "type": "html"},

    {"name": "日本肝臓学会_肝細胞癌診療ガイドライン",
     "url": "https://www.jsh.or.jp/medical/guidelines/jsh_guidlines/medical/",
     "selector": "li, dl, dt, a", "type": "html"},

    {"name": "医学書院_レジデントマニュアル",
     "url": "https://www.igaku-shoin.co.jp/series/739",
     "selector": "div.book-item h3, h3",
     "type": "html"}
]

KEYWORDS = ["ガイドライン", "指針", "診療手引き", "診療指針", "治療指針", "作成指針"]

# =========================
# 日付抽出正規表現
# =========================
DATE_REGEXES = [
    r'(?:発売|発行|刊行|出版|更新|公開)[:：\s]*(\d{4}[年/.\-]\d{1,2}(?:[月/.\-]\d{1,2}日?)?)',
    r'[\(（](\d{4}[年/.\-]\d{1,2})[\)）]',
    r'(\d{4}年\s?\d{1,2}月\s?\d{1,2}日)',
    r'(\d{4}/\d{1,2}/\d{1,2})',
    r'(\d{4}\.\d{1,2}\.\d{1,2})',
    r'(\d{4})(?:年版|版)'
]

HISTORY_FILE = "history.json"
REPORT_FILE = "update_report.csv"
HTML_FILE = "index.html"

# =========================
# Utility
# =========================
def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_history(history):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

def format_date(date_str):
    nums = re.findall(r'\d+', date_str)
    if not nums:
        return "-"
    year = int(nums[0])
    if not (2000 <= year <= 2100):
        return "-"
    month = nums[1].zfill(2) if len(nums) > 1 else "01"
    day = nums[2].zfill(2) if len(nums) > 2 else "01"
    return f"{year}/{month}/{day}"

def extract_date(text):
    for r in DATE_REGEXES:
        m = re.search(r, text)
        if m:
            return format_date(m.group(1))
    return "-"

def clean_title(text):
    text = re.sub(r'ISBN.*', '', text)
    text = re.sub(r'(定価|本体|税込).*', '', text)
    text = re.sub(r'(編集|監修|著者|訳).*', '', text)
    return " ".join(text.split()).strip()

# =========================
# Site checker
# =========================
def check_site(target):
    url = target.get("url")
    if not url and "url_template" in target:
        url = target["url_template"].format(**target.get("version", {}))

    found = []
    try:
        r = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()

        soup = BeautifulSoup(r.content, "html.parser")

        if target["type"] in ("html", "html_versioned"):
            for el in soup.select(target.get("selector", "li")):
                t = el.get_text(" ", strip=True)
                if any(k in t for k in KEYWORDS):
                    found.append({
                        "title": clean_title(t),
                        "pub_date": extract_date(t),
                        "url": url
                    })

        elif target["type"] == "html_pdf_index":
            for a in soup.select(target.get("selector", "a")):
                href = a.get("href", "")
                if not href.lower().endswith(".pdf"):
                    continue
                pdf_url = href if href.startswith("http") else requests.compat.urljoin(url, href)
                title = clean_title(a.get_text(strip=True))
                if not any(k in title for k in KEYWORDS):
                    continue

                h = requests.head(pdf_url, allow_redirects=True, timeout=15)
                lm = h.headers.get("Last-Modified")
                date = "-"
                if lm:
                    dt = email.utils.parsedate_to_datetime(lm).astimezone(JST)
                    date = dt.strftime("%Y/%m/%d")

                found.append({
                    "title": title,
                    "pub_date": date,
                    "url": pdf_url
                })

        elif target["type"] == "pdf_header":
            lm = r.headers.get("Last-Modified")
            date = "-"
            if lm:
                dt = email.utils.parsedate_to_datetime(lm).astimezone(JST)
                date = dt.strftime("%Y/%m/%d")
            found.append({
                "title": f"【PDF更新】{target['name']}",
                "pub_date": date,
                "url": url
            })

    except Exception as e:
        print(f"Error: {target['name']} -> {e}")

    return found

# =========================
# Main
# =========================
def main():
    history = load_history()
    today = datetime.now(JST).strftime("%Y-%m-%d")
    rows = []

    for t in TARGETS:
        print("Checking:", t["name"])
        items = check_site(t)
        history.setdefault(t["name"], [])

        for it in items:
            if it["title"] not in history[t["name"]]:
                rows.append({
                    "ステータス": "★新着",
                    "出版社": t["name"],
                    "発刊日": it["pub_date"],
                    "タイトル内容": it["title"],
                    "URL": it["url"],
                    "検知日": today
                })
                history[t["name"]].append(it["title"])

    save_history(history)

    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    main()
