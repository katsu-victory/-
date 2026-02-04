import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime

# 監視対象の設定
TARGETS = [
    {"name": "医学図書出版", "url": "https://igakutosho.co.jp/collections/book", "type": "html"},
    {"name": "メディカルレビュー社", "url": "https://med.m-review.co.jp/merebo/products/book", "type": "html"},
    {"name": "診断と治療社", "url": "https://www.shindan.co.jp/", "type": "html"},
    {"name": "南江堂", "url": "https://www.nankodo.co.jp/shinkan/list.aspx?div=d", "type": "html"},
    {"name": "医学書院", "url": "https://www.igaku-shoin.co.jp/", "type": "html"},
    {"name": "金原出版(GL)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=02", "type": "html"},
    {"name": "金原出版(規約)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=01", "type": "html"},
    # PDFは中身の解析が難しいため、ファイルの更新状況(ヘッダー)を確認
    {"name": "金原出版(規約PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/ky_new.pdf", "type": "pdf_header"},
    {"name": "金原出版(GL PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/gl_new.pdf", "type": "pdf_header"}
]

KEYWORDS = ["ガイドライン", "規約", "指針"]
HISTORY_FILE = "history.json"
REPORT_FILE = "update_report.csv"

def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_history(history):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

def check_site(target):
    found_items = []
    try:
        response = requests.get(target["url"], timeout=20)
        response.raise_for_status()

        if target["type"] == "html":
            soup = BeautifulSoup(response.content, "html.parser")
            # ページ内のすべてのテキストからキーワードを含む行を探す
            # 多くのサイトに対応するため、広めに取得
            elements = soup.find_all(["a", "div", "span", "h3", "h2"])
            for el in elements:
                text = el.get_text().strip()
                if any(kw in text for kw in KEYWORDS):
                    # 短すぎる、または長すぎるテキストは除外（ノイズ対策）
                    if 5 < len(text) < 100:
                        found_items.append(text)
        
        elif target["type"] == "pdf_header":
            # PDFの場合は、ETagまたはLast-Modifiedを識別子にする
            info = response.headers.get("Last-Modified") or response.headers.get("ETag") or "no-info"
            found_items.append(f"PDF更新情報: {info}")

    except Exception as e:
        print(f"Error checking {target['name']}: {e}")
    
    return list(set(found_items)) # 重複削除

def main():
    history = load_history()
    new_discoveries = []
    
    print(f"--- 巡回開始: {datetime.now()} ---")
    
    for target in TARGETS:
        print(f"Checking {target['name']}...")
        items = check_site(target)
        
        site_name = target["name"]
        if site_name not in history:
            history[site_name] = []
            
        for item in items:
            if item not in history[site_name]:
                print(f"  [新着!] {item}")
                new_discoveries.append({
                    "出版社": site_name,
                    "内容": item,
                    "URL": target["url"],
                    "検知日": datetime.now().strftime("%Y-%m-%d")
                })
                history[site_name].append(item)
    
    save_history(history)
    
    if new_discoveries:
        import pandas as pd
        df = pd.DataFrame(new_discoveries)
        # 既存のレポートがあれば追記、なければ新規作成
        if os.path.exists(REPORT_FILE):
            old_df = pd.read_csv(REPORT_FILE)
            df = pd.concat([old_df, df], ignore_index=True)
        
        df.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")
        print(f"\n{len(new_discoveries)} 件の新着が見つかりました。")
    else:
        print("\n新しいガイドラインは見つかりませんでした。")

if __name__ == "__main__":
    main()
