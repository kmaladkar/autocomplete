#!/usr/bin/env python3
"""Generate a 3-4 MB raw click logs CSV with varied, realistic query/suggestion pairs."""
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Target size in bytes (3.5 MB)
TARGET_BYTES = 3_670_016  # ~3.5 MB

# Robust (prefix, full_suggestion) pairs across many domains
QUERY_SUGGESTIONS = [
    # Tech & dev
    ("py", "python"), ("pyt", "python tutorial"), ("pyth", "python download"), ("git", "github"),
    ("gith", "github login"), ("jav", "javascript"), ("java", "java download"), ("nod", "nodejs"),
    ("react", "react native"), ("vue", "vue js"), ("typ", "typescript"), ("htm", "html"),
    ("css", "css grid"), ("sql", "sql tutorial"), ("red", "redis"), ("dock", "docker"),
    ("kuber", "kubernetes"), ("aws", "aws console"), ("azur", "azure"), ("gcp", "gcp pricing"),
    ("linu", "linux"), ("ubun", "ubuntu"), ("mac", "macbook"), ("vs", "vscode"),
    ("stack", "stackoverflow"), ("npm", "npm install"), ("pip", "pip install"), ("cond", "conda"),
    ("tens", "tensorflow"), ("pytor", "pytorch"), ("mach", "machine learning"), ("neural", "neural network"),
    ("api", "api key"), ("rest", "rest api"), ("graph", "graphql"), ("web", "web development"),
    ("front", "frontend"), ("back", "backend"), ("full", "full stack"), ("devops", "devops tools"),
    ("ci", "ci cd"), ("jenk", "jenkins"), ("terraform", "terraform aws"), ("ansib", "ansible"),
    # Search & productivity
    ("wea", "weather"), ("weat", "weather london"), ("weath", "weather new york"), ("weathe", "weather today"),
    ("gma", "gmail"), ("gmai", "gmail login"), ("goog", "google"), ("googl", "google maps"),
    ("you", "youtube"), ("yout", "youtube music"), ("youtu", "youtube download"), ("face", "facebook"),
    ("inst", "instagram"), ("twit", "twitter"), ("link", "linkedin"), ("netf", "netflix"),
    ("spot", "spotify"), ("amaz", "amazon"), ("ebay", "ebay uk"), ("alie", "aliexpress"),
    ("pay", "paypal"), ("stripe", "stripe api"), ("slack", "slack download"), ("zoom", "zoom meeting"),
    ("teams", "microsoft teams"), ("notion", "notion template"), ("trello", "trello board"),
    ("cal", "calendar"), ("drive", "google drive"), ("drop", "dropbox"), ("one", "onedrive"),
    # Travel & local
    ("flig", "flights"), ("flight", "flights to london"), ("hotel", "hotel near me"), ("book", "booking"),
    ("air", "airbnb"), ("uber", "uber eats"), ("lyft", "lyft ride"), ("map", "google maps"),
    ("rest", "restaurants near me"), ("food", "food delivery"), ("doord", "doordash"), ("grub", "grubhub"),
    ("pizza", "pizza hut"), ("starb", "starbucks"), ("mcd", "mcdonalds"), ("bank", "bank of america"),
    ("atm", "atm near me"), ("gas", "gas station"), ("phar", "pharmacy"), ("hospital", "hospital near me"),
    # Shopping & retail
    ("best", "best buy"), ("walm", "walmart"), ("targ", "target"), ("cost", "costco"),
    ("ikea", "ikea near me"), ("apple", "apple store"), ("sams", "samsung"), ("nike", "nike shoes"),
    ("adid", "adidas"), ("zara", "zara sale"), ("h&m", "h&m"), ("shein", "shein dress"),
    ("coupon", "coupon code"), ("discount", "discount tires"), ("deal", "deals today"),
    # Health & fitness
    ("gym", "gym near me"), ("yoga", "yoga classes"), ("medit", "meditation"), ("run", "running shoes"),
    ("fit", "fitness tracker"), ("diet", "diet plan"), ("calor", "calorie counter"), ("health", "health insurance"),
    ("doctor", "doctor near me"), ("pharmacy", "pharmacy near me"), ("covid", "covid vaccine"),
    ("sympt", "symptoms checker"), ("webmd", "webmd"), ("mayo", "mayo clinic"),
    # Finance & business
    ("stock", "stock market"), ("invest", "investing"), ("crypto", "cryptocurrency"), ("bitcoin", "bitcoin price"),
    ("eth", "ethereum"), ("robin", "robinhood"), ("fidelity", "fidelity login"), ("chase", "chase bank"),
    ("tax", "tax calculator"), ("mortgage", "mortgage calculator"), ("credit", "credit score"),
    ("loan", "loan calculator"), ("savings", "savings account"), ("ira", "ira contribution limit"),
    # Education & reference
    ("wiki", "wikipedia"), ("dict", "dictionary"), ("trans", "translate"), ("convert", "convert currency"),
    ("calc", "calculator"), ("recipe", "recipes"), ("how", "how to"), ("what", "what is"),
    ("why", "why is"), ("when", "when is"), ("where", "where to"), ("who", "who is"),
    ("course", "coursera"), ("udemy", "udemy courses"), ("khan", "khan academy"), ("duo", "duolingo"),
    # Entertainment & news
    ("news", "news today"), ("cnn", "cnn news"), ("bbc", "bbc news"), ("reddit", "reddit"),
    ("game", "games"), ("steam", "steam deck"), ("xbox", "xbox game pass"), ("play", "playstation"),
    ("nintendo", "nintendo switch"), ("movie", "movies"), ("imdb", "imdb top 250"),
    ("song", "song lyrics"), ("shazam", "shazam"), ("podcast", "podcast app"),
    # More tech & tools
    ("pass", "password manager"), ("vpn", "vpn free"), ("antiv", "antivirus"), ("backup", "backup software"),
    ("photo", "photoshop"), ("figma", "figma design"), ("canva", "canva"), ("excel", "excel formula"),
    ("word", "microsoft word"), ("power", "powerpoint"), ("pdf", "pdf converter"), ("zip", "zip file"),
    ("unzip", "unzip online"), ("email", "email template"), ("resume", "resume builder"),
]

# Expand with common prefixes for same suggestions to simulate real typing
EXTRA_PREFIXES = [
    ("we", "weather"), ("we", "weather today"), ("wea", "weather london"), ("weat", "weather nyc"),
    ("py", "python"), ("pyt", "python"), ("pyth", "python"), ("pytho", "python"),
    ("go", "google"), ("goo", "google"), ("goog", "google"), ("googl", "google"),
    ("am", "amazon"), ("ama", "amazon"), ("amaz", "amazon"), ("amazo", "amazon"),
    ("you", "youtube"), ("yout", "youtube"), ("youtu", "youtube"), ("youtub", "youtube"),
    ("fa", "facebook"), ("fac", "facebook"), ("face", "facebook"), ("faceb", "facebook"),
    ("we", "web"), ("we", "website"), ("we", "western union"), ("we", "wells fargo"),
    ("we", "weather channel"), ("we", "weather app"), ("we", "weather forecast"),
]


def random_timestamp(start_days_ago: int = 180) -> str:
    base = datetime.now(timezone.utc)
    delta = timedelta(days=random.randint(0, start_days_ago), seconds=random.randint(0, 86400))
    return (base - delta).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    raw = root / "data" / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    out_path = raw / "click_logs.csv"

    all_pairs = list(QUERY_SUGGESTIONS) + list(EXTRA_PREFIXES)
    rows = []
    bytes_so_far = 0
    header = "query,clicked_suggestion,position,timestamp\n"
    bytes_so_far += len(header)

    while bytes_so_far < TARGET_BYTES:
        query, suggestion = random.choice(all_pairs)
        position = random.choices([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], weights=[40, 25, 15, 8, 5, 3, 2, 1, 1, 0])[0]
        ts = random_timestamp()
        line = f'"{query}","{suggestion}",{position},"{ts}"\n'
        rows.append(line)
        bytes_so_far += len(line)

    with open(out_path, "w") as f:
        f.write(header)
        f.writelines(rows)

    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"Wrote {out_path} ({len(rows):,} rows, {size_mb:.2f} MB)")


if __name__ == "__main__":
    main()
