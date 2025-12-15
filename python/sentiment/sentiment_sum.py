import csv

CSV_FILE_PATH1 = "crypto_news_finbert_sentiment_whale_news.csv"
CSV_FILE_PATH2= "crypto_news_finbert_sentiment.csv"
CSV_FILE_PATH3= "crypto_news_finbert_sentiment_yfinance.csv"
KEYWORD = "Bitcoin"  # single keyword for debugging

def compute_sentiment_sum(csv_path, keyword):
    total_sum = 0
    matched_rows = 0

    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        # print("Detected columns:", reader.fieldnames)
        print("*" * 30)
        print(keyword)
        print("*" * 30)

        for i, row in enumerate(reader, start=1):
            title = (row.get("title") or "")
            description = (row.get("description") or "")
            tags = (row.get("tags") or "")
            sentiment_raw = (row.get("sentiment") or "")

            combined_text = f"{title} {description} {tags}".lower()

            # 🔎 DEBUG: show first few rows
            # if i <= 5:
            #     print("\nROW", i)
            #     print("TITLE:", title[:120])
            #     print("DESCRIPTION:", description[:120])
            #     print("TAGS:", tags)
            #     print("SENTIMENT RAW:", sentiment_raw)

            if keyword.lower() not in combined_text:
                continue

            # ✅ Keyword matched
            matched_rows += 1
            # print("\n✅ KEYWORD MATCHED IN ROW", i)
            # print("Matched text:", combined_text[:200])
            # print("Sentiment raw value:", sentiment_raw)

            s = sentiment_raw.lower()

            if "positive" in s:
                total_sum += 1
                # print(f"→ sentiment +1, total_sum now {total_sum}")
            elif "negative" in s:
                total_sum -= 1
                # print(f"→ sentiment -1, total_sum now {total_sum}")
            elif "neutral" in s:
                # print(f"→ sentiment 0, total_sum now {total_sum}")
                total_sum += 0
            else:
                print("⚠️ sentiment NOT recognized")

    print("\nTotal matched rows:", matched_rows)
    return total_sum


def quick_sentiment_sum(csv_path, keyword):
    """Quick calculation without detailed prints."""
    total_sum = 0

    try:
        with open(csv_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                title = (row.get("title") or "")
                description = (row.get("description") or "")
                tags = (row.get("tags") or "")
                sentiment_raw = (row.get("sentiment") or "")

                combined_text = f"{title} {description} {tags}".lower()

                if keyword.lower() not in combined_text:
                    continue

                s = sentiment_raw.lower()
                if "positive" in s:
                    total_sum += 1
                elif "negative" in s:
                    total_sum -= 1
                # neutral adds 0, so no action needed

        return total_sum
    except FileNotFoundError:
        print(f"File not found: {csv_path}")
        return 0


def simple_total_sentiment_sum(keyword):
    """Simple function that returns total sentiment sum for a keyword."""
    file_paths = [CSV_FILE_PATH1, CSV_FILE_PATH2, CSV_FILE_PATH3]
    total = 0

    for file_path in file_paths:
        try:
            with open(file_path, newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)

                for row in reader:
                    title = (row.get("title") or "")
                    description = (row.get("description") or "")
                    tags = (row.get("tags") or "")
                    sentiment_raw = (row.get("sentiment") or "")

                    combined_text = f"{title} {description} {tags}".lower()

                    if keyword.lower() in combined_text:
                        s = sentiment_raw.lower()
                        if "positive" in s:
                            total += 1
                        elif "negative" in s:
                            total -= 1
        except FileNotFoundError:
            print(f"Warning: {file_path} not found")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    return total


# Usage:
if __name__ == "__main__":
    total = simple_total_sentiment_sum("Ethereum")
    print(f"Total sentiment sum for Ethereum: {total:+d}")

# if __name__ == "__main__":
#     result = compute_sentiment_sum(CSV_FILE_PATH1, KEYWORD)
#     result2 = compute_sentiment_sum(CSV_FILE_PATH2, KEYWORD)
#     print("\nFINAL TOTAL1:", result)
#     print("\nFINAL TOTAL2:", result2)
#
#     files = [
#         ("File 1", CSV_FILE_PATH1),
#         ("File 2", CSV_FILE_PATH2),
#         ("File 3", CSV_FILE_PATH3)
#     ]
#     print(f"Calculating total sentiment sum for '{KEYWORD}'...")
#     print("-" * 50)
#
#     total_sum = 0
#     for name, path in files:
#         file_sum = quick_sentiment_sum(path, KEYWORD)
#         total_sum += file_sum
#         print(f"{name}: {file_sum:+d}")
#
#     print("-" * 50)
#     print(f"TOTAL SUM: {total_sum:+d}")
