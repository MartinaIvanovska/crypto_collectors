import csv

CSV_FILE_PATH1 = "crypto_news_finbert_sentiment_whale_news.csv"
CSV_FILE_PATH2= "crypto_news_finbert_sentiment.csv"
KEYWORD = "Ethereum"  # single keyword for debugging

def compute_sentiment_sum(csv_path, keyword):
    total_sum = 0
    matched_rows = 0

    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        print("Detected columns:", reader.fieldnames)

        for i, row in enumerate(reader, start=1):
            title = (row.get("title") or "")
            description = (row.get("description") or "")
            tags = (row.get("tags") or "")
            sentiment_raw = (row.get("sentiment") or "")

            combined_text = f"{title} {description} {tags}".lower()

            # 🔎 DEBUG: show first few rows
            if i <= 5:
                print("\nROW", i)
                print("TITLE:", title[:120])
                print("DESCRIPTION:", description[:120])
                print("TAGS:", tags)
                print("SENTIMENT RAW:", sentiment_raw)

            if keyword.lower() not in combined_text:
                continue

            # ✅ Keyword matched
            matched_rows += 1
            print("\n✅ KEYWORD MATCHED IN ROW", i)
            print("Matched text:", combined_text[:200])
            print("Sentiment raw value:", sentiment_raw)

            s = sentiment_raw.lower()

            if "positive" in s:
                total_sum += 1
                print(f"→ sentiment +1, total_sum now {total_sum}")
            elif "negative" in s:
                total_sum -= 1
                print(f"→ sentiment -1, total_sum now {total_sum}")
            elif "neutral" in s:
                print(f"→ sentiment 0, total_sum now {total_sum}")

            else:
                print("⚠️ sentiment NOT recognized")

    print("\nTotal matched rows:", matched_rows)
    return total_sum


if __name__ == "__main__":
    result = compute_sentiment_sum(CSV_FILE_PATH1, KEYWORD)
    result2 = compute_sentiment_sum(CSV_FILE_PATH2, KEYWORD)
    print("\nFINAL TOTAL1:", result)
    print("\nFINAL TOTAL2:", result2)
