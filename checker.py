import requests
from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import re
from datetime import datetime, timezone, timedelta
import email.utils

# タイムゾーンの設定（日本時間 JST = UTC+9）
JST = timezone(timedelta(hours=+9), 'JST')

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

KEYWORDS = ["ガイドライン", "指針", "診療手引き", "診療指針", "治療指針", "作成指針"]

# 日付抽出用のパターン（優先度順）
DATE_REGICES = [
    # 1. 明示的なラベル付き（最優先）
    r'(?:発売|発行|刊行|出版|更新|公開)(?:日|年月)?[:：\s]*(\d{4}[年/.\-]\d{1,2}(?:[月/.\-]\d{1,2}日?)?)',
    # 2. カッコ内の日付（(2024/12) など）
    r'[\(（](\d{4}[年/.\-]\d{1,2}(?:[月/.\-]\d{1,2}日?)?)[\)）]',
    # 3. 一般的な日付形式
    r'(\d{4}年\s?\d{1,2}月\s?\d{1,2}日)',
    r'(\d{4}/\d{1,2}/\d{1,2})',
    r'(\d{4}\.\d{1,2}\.\d{1,2})',
    # 4. 年月のみ
    r'(\d{4}年\s?\d{1,2}月)',
    r'(\d{4}/\d{1,2})',
    r'(\d{4}\.\d{1,2})',
    # 5. タイトル内によくある「2024年版」などの年号（最終手段）
    r'(\d{4})(?:年版|版)'
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
    """日付文字列を YYYY/MM/DD に整形する。不正な日付は弾く。"""
    if not date_str or date_str == "-": return "-"
    
    nums = re.findall(r'\d+', date_str)
    if len(nums) >= 1:
        year = int(nums[0])
        # 異常な西暦は除外
        if not (2000 <= year <= 2100): return "-"
        
        month = nums[1].zfill(2) if len(nums) > 1 else "01"
        day = nums[2].zfill(2) if len(nums) > 2 else "01"
        
        # 月日の妥当性チェック
        if not (1 <= int(month) <= 12): month = "01"
        if not (1 <= int(day) <= 31): day = "01"
        
        return f"{year}/{month}/{day}"
    return "-"

def extract_date_stricter(text):
    """複数のパターンを試し、最も妥当な日付を抽出する"""
    for pattern in DATE_REGICES:
        match = re.search(pattern, text)
        if match:
            extracted = match.group(1).strip()
            formatted = format_date_string(extracted)
            if formatted != "-":
                return formatted
    return "-"

def clean_title(text):
    """タイトルの表示を綺麗にする。ノイズと抽出済みの付随情報を削る。"""
    # ISBN
    text = re.sub(r'ISBN\s?[:：]?\s?(97[89][- ]?)?([0-9Xx][- ]?){9,13}', '', text)
    # 価格
    text = re.sub(r'(定価|本体|税込|税別)[:：]?\s?[0-9,]+円?.*', '', text)
    # 著者・編集情報が長すぎる場合
    text = re.sub(r'(?:編集|発行|著者|訳|監修)\)?[:：].*', '', text)
    # 発売日などのラベル単体が残った場合
    text = re.sub(r'(?:発売日|発行日|刊行日|出版日|更新日)[:：]\s*$', '', text)
    
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
                        if 10 < len(text) < 800:
                            pub_date = extract_date_stricter(text)
                            cleaned = clean_title(text)
                            
                            # タイトルから特定の日付文字列を除去
                            if pub_date != "-":
                                date_parts = pub_date.split('/')
                                for p in date_parts:
                                    cleaned = cleaned.replace(p, "").replace(f"{int(p)}", "")
                            
                            title_part = cleaned[:200]
                            if len(title_part) > 5:
                                found_items.append({
                                    "title": title_part,
                                    "pub_date": pub_date
                                })
            
            # フォールバック
            if not found_items:
                for tag in soup.find_all(["a", "h2", "h3", "h4"]):
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
                try:
                    dt = email.utils.parsedate_to_datetime(last_mod)
                    # 日本時間に変換
                    dt_jst = dt.astimezone(JST)
                    date_val = dt_jst.strftime("%Y/%m/%d")
                except:
                    date_val = "-"
            
            found_items.append({
                "title": "【PDFファイル更新監視】" + target["name"],
                "pub_date": date_val
            })
    except Exception as e:
        print(f"Error checking {target['name']}: {e}")
    return found_items

def generate_html(df):
    """リッチでソート可能なダッシュボードHTMLを生成。"""
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
    
    display_data = []
    for _, row in df.iterrows():
        title = "-"
        for col in ["タイトル内容", "内容", "GL名"]:
            if col in row and pd.notna(row[col]) and row[col] != "-":
                title = str(row[col])
                break
        
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
        <title>診療ガイドライン・規約 新着監視</title>
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;700;900&display=swap');
            body {{ font-family: 'Noto Sans JP', sans-serif; }}
            .status-new {{ background: linear-gradient(135deg, #ff416c 0%, #ff4b2b 100%); color: white; }}
            .status-old {{ background-color: #f3f4f6; color: #6b7280; }}
            .pub-date-box {{ font-family: 'Courier New', Courier, monospace; }}
            th.sortable {{ cursor: pointer; position: relative; }}
            th.sortable:hover {{ background-color: #1e3a8a; }}
            th.sortable::after {{ content: ' ↕'; font-size: 0.8em; opacity: 0.5; }}
        </style>
    </head>
    <body class="bg-gray-100 p-4 md:p-10 text-gray-800">
        <div class="max-w-7xl mx-auto">
            <header class="flex flex-col md:flex-row justify-between items-center mb-10 gap-4 border-b-4 border-blue-800 pb-6">
                <div>
                    <h1 class="text-4xl font-black text-blue-900 tracking-tighter">診療ガイドライン新着監視</h1>
                    <p class="text-gray-500 font-bold mt-1 uppercase tracking-widest text-xs">Medical Guideline Monitoring System</p>
                </div>
                <div class="bg-white px-6 py-2 rounded-full shadow-inner border border-gray-200">
                    <span class="text-xs text-gray-400 block font-bold">最終巡回日時 (JST)</span>
                    <span class="font-mono text-sm font-bold text-blue-700">{now}</span>
                </div>
            </header>
            
            <div class="bg-white shadow-2xl rounded-3xl overflow-hidden border border-gray-200">
                <div class="overflow-x-auto">
                    <table class="min-w-full divide-y divide-gray-200" id="mainTable">
                        <thead>
                            <tr class="bg-blue-900 text-white shadow-md">
                                <th onclick="sortTable(0)" class="sortable px-6 py-5 text-left text-xs font-black uppercase tracking-tighter">Status</th>
                                <th onclick="sortTable(1)" class="sortable px-6 py-5 text-left text-xs font-black uppercase tracking-tighter">Publisher</th>
                                <th onclick="sortTable(2)" class="sortable px-6 py-5 text-left text-xs font-black uppercase tracking-tighter text-blue-200">Pub Date</th>
                                <th onclick="sortTable(3)" class="sortable px-6 py-5 text-left text-xs font-black uppercase tracking-tighter">Content Title</th>
                                <th onclick="sortTable(4)" class="sortable px-6 py-5 text-left text-xs font-black uppercase tracking-tighter text-blue-200">Detected</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-gray-100">
    """
    for row in display_data:
        is_new = "新着" in row['status']
        row_cls = "bg-red-50/70" if is_new else ""
        badge_cls = "status-new shadow-sm" if is_new else "status-old"
        
        html_content += f"""
                            <tr class="hover:bg-blue-50 transition-all {row_cls}">
                                <td class="px-6 py-5 whitespace-nowrap"><span class="px-4 py-1.5 rounded-full text-[10px] font-black {badge_cls}">{row['status']}</span></td>
                                <td class="px-6 py-5 whitespace-nowrap text-sm font-black text-gray-600">{row['publisher']}</td>
                                <td class="px-6 py-5 whitespace-nowrap text-sm pub-date-box text-blue-800 font-bold">{row['pub_date']}</td>
                                <td class="px-6 py-5 text-sm leading-relaxed">
                                    <a href="{row['url']}" target="_blank" class="text-blue-600 hover:text-blue-900 font-bold border-b border-transparent hover:border-blue-900 pb-0.5 transition-all">
                                        {row['title']}
                                    </a>
                                </td>
                                <td class="px-6 py-5 whitespace-nowrap text-xs text-gray-400 font-mono">{row['detect_date']}</td>
                            </tr>
        """
    html_content += """
                        </tbody>
                    </table>
                </div>
            </div>
            <footer class="mt-12 text-center text-gray-400 text-xs font-bold italic">
                &copy; 2026 診療ガイドライン更新監視システム | 毎日JST AM8:00自動更新
            </footer>
        </div>
        
        <script>
        function sortTable(n) {
            var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
            table = document.getElementById("mainTable");
            switching = true;
            dir = "asc";
            while (switching) {
                switching = false;
                rows = table.rows;
                for (i = 1; i < (rows.length - 1); i++) {
                    shouldSwitch = false;
                    x = rows[i].getElementsByTagName("TD")[n];
                    y = rows[i + 1].getElementsByTagName("TD")[n];
                    if (dir == "asc") {
                        if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
                            shouldSwitch = true;
                            break;
                        }
                    } else if (dir == "desc") {
                        if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
                            shouldSwitch = true;
                            break;
                        }
                    }
                }
                if (shouldSwitch) {
                    rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                    switching = true;
                    switchcount++;
                } else {
                    if (switchcount == 0 && dir == "asc") {
                        dir = "desc";
                        switching = true;
                    }
                }
            }
        }
        </script>
    </body>
    </html>
    """
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html_content)

def main():
    history = load_history()
    new_discoveries = []
    # 日本時間の日付を取得
    today = datetime.now(JST).strftime("%Y-%m-%d")
    
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
    
    # 既存レポートとの統合
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
        generate_html(df.head(250))
    
if __name__ == "__main__":
    main()
