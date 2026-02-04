import requests
from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import re
from datetime import datetime

# 監視対象の設定
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

KEYWORDS = ["ガイドライン", "規約", "指針", "診療手引き", "診療指針", "治療指針", "作成指針"]
HISTORY_FILE = "history.json"
REPORT_FILE = "update_report.csv"
HTML_FILE = "index.html"

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

def clean_title(text):
    text = re.sub(r'ISBN\s?[:：]?\s?(97[89][- ]?)?([0-9Xx][- ]?){9,13}', '', text)
    text = re.sub(r'(定価|本体|税込|税別)[:：]?\s?[0-9,]+円?.*', '', text)
    text = re.sub(r'(編集|発行|著者|訳)\)?[:：].*', '', text)
    text = " ".join(text.split())
    return text.strip()

def check_site(target):
    found_items = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(target["url"], headers=headers, timeout=25)
        response.raise_for_status()

        if target["type"] == "html":
            soup = BeautifulSoup(response.content, "html.parser")
            selector = target.get("selector", "li, tr, div")
            for element in soup.select(selector):
                text = element.get_text(separator=" ").strip()
                if any(kw in text for kw in KEYWORDS):
                    if 8 < len(text) < 500:
                        full_text = clean_title(text)
                        found_items.append({"title": full_text[:150]})
        elif target["type"] == "pdf_header":
            last_mod = response.headers.get("Last-Modified")
            if last_mod:
                found_items.append({"title": "【PDF更新】" + target["name"]})
    except Exception as e:
        print(f"Error checking {target['name']}: {e}")
    return found_items

def generate_html(df):
    """CSVからHTMLを生成。列名の不一致を補正する。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # 列名の正規化（古い「内容」列などを「タイトル内容」に統一）
    rename_map = {"内容": "タイトル内容", "GL名": "タイトル内容"}
    df = df.rename(columns=rename_map)
    
    # 必要な列が欠けている場合の補完
    for col in ["ステータス", "出版社", "タイトル内容", "検知日"]:
        if col not in df.columns:
            df[col] = "-"
    
    df = df.fillna("-")

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>ガイドライン新着状況</title>
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
    </head>
    <body class="bg-gray-50 p-4 md:p-8 font-sans">
        <div class="max-w-6xl mx-auto">
            <h1 class="text-3xl font-extrabold mb-2 text-blue-900">診療ガイドライン新着監視</h1>
            <p class="text-gray-500 mb-8">最終更新: {now}</p>
            <div class="bg-white shadow-xl rounded-xl overflow-hidden border border-gray-200">
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-100">
                        <tr>
                            <th class="px-6 py-4 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">状態</th>
                            <th class="px-6 py-4 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">出版社</th>
                            <th class="px-6 py-4 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">タイトル・内容</th>
                            <th class="px-6 py-4 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">検知日</th>
                        </tr>
                    </thead>
                    <tbody class="bg-white divide-y divide-gray-200">
    """
    for _, row in df.iterrows():
        status = str(row['ステータス'])
        status_cls = "bg-red-500 text-white" if "新着" in status else "bg-gray-200 text-gray-600"
        url = row['URL'] if 'URL' in row and row['URL'] != "-" else "#"
        
        html_content += f"""
                        <tr class="hover:bg-gray-50 transition-colors">
                            <td class="px-6 py-4 whitespace-nowrap"><span class="px-3 py-1 rounded-full text-xs font-bold {status_cls}">{status}</span></td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm font-bold text-gray-800">{row['出版社']}</td>
                            <td class="px-6 py-4 text-sm text-gray-700">
                                <a href="{url}" target="_blank" class="text-blue-600 hover:text-blue-800 font-medium">
                                    {row['タイトル内容']}
                                </a>
                            </td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{row['検知日']}</td>
                        </tr>
        """
    html_content += """
                    </tbody>
                </table>
            </div>
            <div class="mt-6 text-center text-gray-400 text-xs">
                ※URLをクリックすると各サイトの新着ページへ飛びます
            </div>
        </div>
    </body>
    </html>
    """
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html_content)

def main():
    history = load_history()
    new_discoveries = []
    today = datetime.now().strftime("%Y-%m-%d")
    
    for target in TARGETS:
        site_name = target["name"]
        items = check_site(target)
        if site_name not in history: history[site_name] = []
        for item in items:
            title = item["title"]
            if title not in history[site_name]:
                new_discoveries.append({
                    "ステータス": "★新着", "出版社": site_name, "タイトル内容": title,
                    "URL": target["url"], "検知日": today
                })
                history[site_name].append(title)
    
    save_history(history)
    
    if os.path.exists(REPORT_FILE):
        try:
            old_df = pd.read_csv(REPORT_FILE)
            # 古いデータのステータス更新
            if "ステータス" in old_df.columns:
                old_df["ステータス"] = "既知"
            # 新旧データの統合
            df = pd.concat([pd.DataFrame(new_discoveries), old_df], ignore_index=True)
        except:
            df = pd.DataFrame(new_discoveries)
    else:
        df = pd.DataFrame(new_discoveries)
    
    if not df.empty:
        # 重複行を削除（タイトル内容が同じものはまとめる）
        if "タイトル内容" in df.columns:
            df = df.drop_duplicates(subset=["タイトル内容"], keep="first")
        elif "内容" in df.columns:
            df = df.drop_duplicates(subset=["内容"], keep="first")
            
        df.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")
        generate_html(df.head(100))
    
if __name__ == "__main__":
    main()
