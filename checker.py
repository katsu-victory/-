import requests
from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import re
from datetime import datetime

# 監視対象の設定（サイトごとの特性に合わせたセレクタを追加）
TARGETS = [
    {"name": "医学図書出版", "url": "https://igakutosho.co.jp/collections/book", "selector": "h3", "type": "html"},
    {"name": "メディカルレビュー社", "url": "https://med.m-review.co.jp/merebo/products/book", "selector": ".name", "type": "html"},
    {"name": "診断と治療社", "url": "https://www.shindan.co.jp/", "selector": "dt", "type": "html"},
    {"name": "南江堂", "url": "https://www.nankodo.co.jp/shinkan/list.aspx?div=d", "selector": "tr", "type": "html"},
    {"name": "医学書院", "url": "https://www.igaku-shoin.co.jp/", "selector": ".book-title, .title", "type": "html"},
    {"name": "金原出版(GL検索)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=02", "selector": "h4", "type": "html"},
    {"name": "金原出版(規約検索)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=01", "selector": "h4", "type": "html"},
    {"name": "金原出版(お知らせ)", "url": "https://www.kanehara-shuppan.co.jp/news/index.html?no=151", "selector": "dt, dd", "type": "html"},
    {"name": "金原出版(規約PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/ky_new.pdf", "type": "pdf_header"},
    {"name": "金原出版(GL PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/gl_new.pdf", "type": "pdf_header"}
]

# 判定キーワード（バリエーションを増加）
KEYWORDS = ["ガイドライン", "規約", "指針", "診療手引き", "診療指針", "治療指針", "作成指針"]
# 日付抽出用正規表現
DATE_PATTERN = r'(\d{4}[年/]\d{1,2}[月/]\d{1,2}日?)'
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
    match = re.search(DATE_PATTERN, text)
    return match.group(1) if match else "日付不明"

def clean_title(text):
    """ISBNや価格、余計な改行などのノイズを除去する"""
    text = re.sub(r'ISBN:?\s?[\d-]+', '', text)
    text = re.sub(r'定価:?[\d,]+円?（税込）', '', text)
    text = re.sub(r'本体:?[\d,]+円', '', text)
    text = " ".join(text.split()) # 改行と重複スペースを削除
    return text.strip()

def check_site(target):
    found_items = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(target["url"], headers=headers, timeout=25)
        response.raise_for_status()

        if target["type"] == "html":
            soup = BeautifulSoup(response.content, "html.parser")
            # 指定されたセレクタ、または汎用的なリストタグを走査
            selector = target.get("selector", "li, tr, div")
            for element in soup.select(selector):
                text = element.get_text(separator=" ").strip()
                if any(kw in text for kw in KEYWORDS):
                    # 極端に短い、またはメニュー項目のようなものは除外
                    if 8 < len(text) < 400:
                        full_text = clean_title(text)
                        date_str = extract_date(full_text)
                        # 長すぎる場合はタイトル部分だけを切り出す（最初の150文字程度）
                        short_title = full_text[:150]
                        found_items.append({"title": short_title, "date": date_str})
            
            # セレクタで見つからない場合のバックアップ（aタグを直接探す）
            if not found_items:
                for link in soup.find_all("a"):
                    text = link.get_text().strip()
                    if any(kw in text for kw in KEYWORDS) and len(text) > 8:
                        found_items.append({"title": clean_title(text), "date": "日付不明"})
        
        elif target["type"] == "pdf_header":
            last_mod = response.headers.get("Last-Modified")
            if last_mod:
                found_items.append({"title": "PDFファイル更新検知 (金原出版)", "date": last_mod})

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
            
            # 履歴チェック（部分一致ではなく完全一致で厳密に）
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
                # 既存の「★新着」を「既知」に変更
                if "ステータス" in old_df.columns:
                    old_df["ステータス"] = "既知"
                # 新しい発見を一番上に持ってくる
                combined_df = pd.concat([new_df, old_df], ignore_index=True)
            except:
                combined_df = new_df
        else:
            combined_df = new_df
            
        combined_df.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")
        print(f"\n{len(new_discoveries)} 件の新しい情報を検知しました。")
    else:
        print("\n新しい情報は検知されませんでした。")

if __name__ == "__main__":
    main()
