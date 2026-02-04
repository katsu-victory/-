import requests
from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import re
from datetime import datetime

# 監視対象の設定
TARGETS = [
    {"name": "医学図書出版", "url": "https://igakutosho.co.jp/collections/book", "type": "html"},
    {"name": "メディカルレビュー社", "url": "https://med.m-review.co.jp/merebo/products/book", "type": "html"},
    {"name": "診断と治療社", "url": "https://www.shindan.co.jp/", "type": "html"},
    {"name": "南江堂", "url": "https://www.nankodo.co.jp/shinkan/list.aspx?div=d", "type": "html"},
    {"name": "医学書院", "url": "https://www.igaku-shoin.co.jp/", "type": "html"},
    {"name": "金原出版(GL検索)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=02", "type": "html"},
    {"name": "金原出版(規約検索)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=01", "type": "html"},
    {"name": "金原出版(お知らせ)", "url": "https://www.kanehara-shuppan.co.jp/news/index.html?no=151", "type": "html"},
    {"name": "金原出版(規約PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/ky_new.pdf", "type": "pdf_header"},
    {"name": "金原出版(GL PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/gl_new.pdf", "type": "pdf_header"}
]

KEYWORDS = ["ガイドライン", "規約", "指針", "診療手引き"]
DATE_PATTERN = r'(\d{4}[年/]\d{1,2}[月/]\d{1,2}日?)' # 2024年1月1日 や 2024/01/01 を探す
HISTORY_FILE = "history.json"
REPORT_FILE = "update_report.csv"

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_history(history):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

def extract_date(text):
    """テキストから日付っぽい部分を抜き出す"""
    match = re.search(DATE_PATTERN, text)
    return match.group(1) if match else "日付不明"

def check_site(target):
    found_items = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(target["url"], headers=headers, timeout=20)
        response.raise_for_status()

        if target["type"] == "html":
            soup = BeautifulSoup(response.content, "html.parser")
            # 出版社のサイト構造に合わせて、ブロック単位で探す試み
            # 多くのサイトで共通の「リスト項目」になりそうなタグを走査
            for container in soup.find_all(["li", "tr", "div", "article"]):
                text = container.get_text(separator=" ").strip()
                if any(kw in text for kw in KEYWORDS):
                    if 10 < len(text) < 300:
                        # 複数行を1行にまとめ、余計な空白を削除
                        clean_text = " ".join(text.split())
                        # そのブロック内に日付があれば抽出
                        date_str = extract_date(clean_text)
                        found_items.append({"title": clean_text, "date": date_str})
        
        elif target["type"] == "pdf_header":
            last_mod = response.headers.get("Last-Modified")
            if last_mod:
                found_items.append({"title": "PDFファイル更新", "date": last_mod})

    except Exception as e:
        print(f"Error checking {target['name']}: {e}")
    
    # 重複削除
    unique_items = {item['title']: item for item in found_items}.values()
    return list(unique_items)

def main():
    history = load_history()
    new_discoveries = []
    today = datetime.now().strftime("%Y-%m-%d")
    
    print(f"--- 巡回開始: {today} ---")
    
    for target in TARGETS:
        site_name = target["name"]
        print(f"Checking {site_name}...")
        items = check_site(target)
        
        if site_name not in history:
            history[site_name] = []
            
        for item in items:
            title = item["title"]
            pub_date = item["date"]
            
            if title not in history[site_name]:
                new_discoveries.append({
                    "ステータス": "★新着",
                    "出版社": site_name,
                    "出版年月日(推定)": pub_date,
                    "タイトル内容": title,
                    "URL": target["url"],
                    "検知日": today
                })
                history[site_name].append(title)
    
    save_history(history)
    
    if new_discoveries:
        new_df = pd.DataFrame(new_discoveries)
        
        if os.path.exists(REPORT_FILE):
            try:
                old_df = pd.read_csv(REPORT_FILE)
                # 既存データのステータスを「既知」に更新して、古い発見も残す
                if "ステータス" in old_df.columns:
                    old_df["ステータス"] = "既知"
                combined_df = pd.concat([new_df, old_df], ignore_index=True)
            except:
                combined_df = new_df
        else:
            combined_df = new_df
            
        combined_df.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")
        print(f"\n{len(new_discoveries)} 件の新着を検知しました。")
    else:
        print("\n新しい情報は検知されませんでした。")

if __name__ == "__main__":
    main()
