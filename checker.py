import requests
from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import re
from datetime import datetime
import email.utils

# 監視対象の設定
TARGETS = [
    {"name": "医学図書出版", "url": "https://igakutosho.co.jp/collections/book", "selector": "div.grid-view-item, .product-card", "type": "html"},
    {"name": "メディカルレビュー社", "url": "https://med.m-review.co.jp/merebo/products/book", "selector": ".product_list_item, li", "type": "html"},
    {"name": "診断と治療社", "url": "https://www.shindan.co.jp/", "selector": "dl, dt, li", "type": "html"},
    {"name": "南江堂", "url": "https://www.nankodo.co.jp/shinkan/list.aspx?div=d", "selector": "tr, div.shinkan-item", "type": "html"},
    {"name": "医学書院", "url": "https://www.igaku-shoin.co.jp/", "selector": "div.book-item, li", "type": "html"},
    {"name": "金原出版(GL検索)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=02", "selector": "div.book_list_item, tr, li", "type": "html"},
    {"name": "金原出版(規約検索)", "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=01", "selector": "div.book_list_item, tr, li", "type": "html"},
    {"name": "金原出版(お知らせ)", "url": "https://www.kanehara-shuppan.co.jp/news/index.html?no=151", "selector": "dl > *, li", "type": "html"},
    {"name": "金原出版(規約PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/ky_new.pdf", "type": "pdf_header"},
    {"name": "金原出版(GL PDF)", "url": "https://www.kanehara-shuppan.co.jp/_data/books/gl_new.pdf", "type": "pdf_header"}
]

KEYWORDS = ["ガイドライン", "規約", "指針", "診療手引き", "診療指針", "治療指針", "作成指針"]

# 日付抽出用の厳格なパターン
# 1. 発売/発行などのキーワード付き
STRICT_DATE_PATTERNS = [
    r'(?:発売|発行|刊行|出版|更新|公開)(?:日|年月)?[:：\s]?(\d{4}[年/.\-]\d{1,2}(?:[月/.\-]\d{1,2}日?)?)',
    r'(\d{4}年\s?\d{1,2}月\s?\d{1,2}日)',
    r'(\d{4}/\d{1,2}/\d{1,2})',
    r'(\d{4}\.\d{1,2}\.\d{1,2})',
    r'(\d{4}年\d{1,2}月)', # 月まで
]

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

def format_date_string(date_str):
    """日付文字列を YYYY/MM/DD に整形を試みる"""
    if not date_str or date_str == "-": return "-"
    # 数字以外をセパレータに置換
    nums = re.findall(r'\d+', date_str)
    if len(nums) >= 2:
        year = nums[0]
        month = nums[1].zfill(2)
        day = nums[2].zfill(2) if len(nums) > 2 else "01"
        # 異常な数値（西暦が1900年以前や2100年以降）は除外
        if not (1990 <= int(year) <= 2100): return "-"
        return f"{year}/{month}/{day}"
    return date_str

def extract_date_stricter(text):
    """より厳格に、文脈を考慮して日付を抽出する"""
    for pattern in STRICT_DATE_PATTERNS:
        match = re.search(pattern, text)
        if match:
            extracted = match.group(1).strip()
            formatted = format_date_string(extracted)
            if formatted != "-":
                return formatted
    return "-"

def clean_title(text):
    # タイトル内の余計な情報を削る
    text = re.sub(r'ISBN\s?[:：]?\s?(97[89][- ]?)?([0-9Xx][- ]?){9,13}', '', text)
    text = re.sub(r'(定価|本体|税込|税別)[:：]?\s?[0-9,]+円?.*', '', text)
    text = re.sub(r'(編集|発行|著者|訳|監修)\)?[:：].*', '', text)
    # 既に見つかった日付部分もタイトルからは削る（重複防止）
    for pattern in STRICT_DATE_PATTERNS:
        text = re.sub(pattern, '', text)
    
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
            selectors = target.get("selector", "li, tr, div").split(",")
            for sel in selectors:
                for element in soup.select(sel.strip()):
                    text = element.get_text(separator=" ").strip()
                    if any(kw in text for kw in KEYWORDS):
                        if 10 < len(text) < 600:
                            pub_date = extract_date_stricter(text)
                            cleaned = clean_title(text)
                            title_part = cleaned[:200]
                            if len(title_part) > 5:
                                found_items.append({
                                    "title": title_part,
                                    "pub_date": pub_date
                                })
            
            # フォールバック
            if not found_items:
                for tag in soup.find_all(["a", "h3", "h4"]):
                    t = tag.get_text().strip()
                    if any(kw in t for kw in KEYWORDS) and len(t) > 8:
                        found_items.append({
                            "title": clean_title(t), 
                            "pub_date": extract_date_stricter(t)
                        })

        elif target["type"] == "pdf_header":
            last_mod = response.headers.get("Last-Modified")
            date_val = "-"
            if last_mod:
                # RFC形式の日付を YYYY/MM/DD に変換
                dt = email.utils.parsedate_to_datetime(last_mod)
                date_val = dt.strftime("%Y/%m/%d")
            
            found_items.append({
                "title": "【PDF更新監視】" + target["name"],
                "pub_date": date_val
            })
    except Exception as e:
        print(f"Error checking {target['name']}: {e}")
    return found_items

def generate_html(df):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    display_data = []
    for _, row in df.iterrows():
        title = "-"
        for col in ["タイトル内容", "内容", "GL名"]:
            if col in row and pd.notna(row[col]) and row[col] != "-":
                title = str(row[col])
                break
        
        # 表示データの整理
        display_data.append({
            "status": str(row.get("ステータス", "既知")),
            "publisher": str(row.get("出版社", "-")),
            "pub_date": str(row.get("発刊日", "-")),
            "title": title,
            "url": str(row.get("URL", "#")),
            "detect_date": str(row.get("検知日", "-"))
        })

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>診療ガイドライン新着監視</title>
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
        <style>
            .status-new {{ background-color: #ef4444; color: white; }}
            .status-old {{ background-color: #f3f4f6; color: #6b7280; }}
        </style>
    </head>
    <body class="bg-gray-50 p-4 md:p-8 font-sans text-gray-900">
        <div class="max-w-7xl mx-auto">
            <header class="flex justify-between items-end mb-8 pb-4 border-b-2 border-blue-900">
                <div>
                    <h1 class="text-3xl font-black text-blue-900">診療ガイドライン新着監視</h1>
                    <p class="text-gray-500 mt-1">出版各社の新刊・更新情報を自動集約</p>
                </div>
                <div class="text-right">
                    <p class="text-xs text-gray-400">最終巡回日時</p>
                    <p class="font-mono text-sm font-bold">{now}</p>
                </div>
            </header>
            
            <div class="bg-white shadow-2xl rounded-2xl overflow-hidden">
                <table class="min-w-full divide-y divide-gray-200">
                    <thead>
                        <tr class="bg-blue-900 text-white">
                            <th class="px-6 py-4 text-left text-xs font-bold uppercase tracking-widest">状態</th>
                            <th class="px-6 py-4 text-left text-xs font-bold uppercase tracking-widest">出版社</th>
                            <th class="px-6 py-4 text-left text-xs font-bold uppercase tracking-widest">発刊日</th>
                            <th class="px-6 py-4 text-left text-xs font-bold uppercase tracking-widest">タイトル・内容</th>
                            <th class="px-6 py-4 text-left text-xs font-bold uppercase tracking-widest whitespace-nowrap">検知日</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-gray-100">
    """
    for row in display_data:
        is_new = "新着" in row['status']
        row_cls = "bg-red-50/50" if is_new else ""
        badge_cls = "status-new" if is_new else "status-old"
        
        html_content += f"""
                        <tr class="hover:bg-blue-50 transition-colors {row_cls}">
                            <td class="px-6 py-4 whitespace-nowrap"><span class="px-3 py-1 rounded-full text-xs font-black {badge_cls}">{row['status']}</span></td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm font-bold text-gray-700">{row['publisher']}</td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm font-mono text-blue-800 font-bold">{row['pub_date']}</td>
                            <td class="px-6 py-4 text-sm">
                                <a href="{row['url']}" target="_blank" class="text-blue-600 hover:text-blue-900 font-semibold decoration-blue-200 decoration-2 underline-offset-4 hover:underline">
                                    {row['title']}
                                </a>
                            </td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-400 font-mono">{row['detect_date']}</td>
                        </tr>
        """
    html_content += """
                    </tbody>
                </table>
            </div>
            <footer class="mt-8 text-center text-gray-400 text-xs">
                &copy; 2026 診療ガイドライン更新監視システム | 自動実行中
            </footer>
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
        print(f"Checking {site_name}...")
        items = check_site(target)
        if site_name not in history: history[site_name] = []
        
        for item in items:
            title = item["title"]
            pub_date = item.get("pub_date", "-")
            if title not in history[site_name]:
                new_discoveries.append({
                    "ステータス": "★新着", "出版社": site_name, "発刊日": pub_date,
                    "タイトル内容": title, "URL": target["url"], "検知日": today
                })
                history[site_name].append(title)
    
    save_history(history)
    
    if os.path.exists(REPORT_FILE):
        try:
            old_df = pd.read_csv(REPORT_FILE)
            rename_map = {"内容": "タイトル内容", "GL名": "タイトル内容", "確認日時": "検知日"}
            old_df = old_df.rename(columns=rename_map)
            if "ステータス" in old_df.columns:
                old_df["ステータス"] = "既知"
            
            valid_cols = ["ステータス", "出版社", "発刊日", "タイトル内容", "URL", "検知日"]
            for col in valid_cols:
                if col not in old_df.columns: old_df[col] = "-"
            
            old_df = old_df[valid_cols]
            new_df = pd.DataFrame(new_discoveries)
            if not new_df.empty:
                df = pd.concat([new_df, old_df], ignore_index=True)
            else:
                df = old_df
        except:
            df = pd.DataFrame(new_discoveries)
    else:
        df = pd.DataFrame(new_discoveries)
    
    if not df.empty:
        df = df.drop_duplicates(subset=["タイトル内容"], keep="first")
        df.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")
        generate_html(df.head(200))
    
if __name__ == "__main__":
    main()
