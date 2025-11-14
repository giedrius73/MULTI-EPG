import requests, gzip
from io import BytesIO
from lxml import etree
import pytz
from datetime import datetime

LANG_PRIORITY = ["lt", "ru", "en"]

def load_sources(path="sources.txt"):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def fetch_xml(url):
    print(f"Downloading: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    content = r.content
    # jei failas gz, išpakuojam
    if url.endswith(".gz") or content[:2] == b"\x1f\x8b":
        content = gzip.GzipFile(fileobj=BytesIO(content)).read()
    return etree.fromstring(content)

def pick_lang(elements):
    if not elements:
        return None
    by_lang = {}
    for el in elements:
        lang = el.get("lang", "").lower()
        by_lang.setdefault(lang, []).append(el)
    for lang in LANG_PRIORITY:
        if lang in by_lang:
            return by_lang[lang][0]
    return elements[0]

def normalize_time(timestr):
    # XMLTV laikas pvz: 20251114060000 +0000
    dt = datetime.strptime(timestr[:14], "%Y%m%d%H%M%S")
    tz_offset = timestr[15:]
    if tz_offset:
        sign = 1 if tz_offset[0] == "+" else -1
        hours = int(tz_offset[1:3])
        minutes = int(tz_offset[3:5])
        dt = dt.replace(tzinfo=pytz.FixedOffset(sign*(hours*60+minutes)))
    else:
        dt = dt.replace(tzinfo=pytz.UTC)
    vilnius = pytz.timezone("Europe/Vilnius")
    dt_vilnius = dt.astimezone(vilnius)
    return dt_vilnius.strftime("%Y%m%d%H%M%S %z")

def merge_sources(sources):
    tv = etree.Element("tv")
    channel_index = {}
    programme_index = {}

    for url in sources:
        try:
            doc = fetch_xml(url)
        except Exception as e:
            print(f"Failed to download {url}: {e}")
            continue
        for ch in doc.findall("channel"):
            ch_id = ch.get("id")
            if ch_id not in channel_index:
                channel_index[ch_id] = ch
        for p in doc.findall("programme"):
            ch_id = p.get("channel")
            start = normalize_time(p.get("start"))
            stop = normalize_time(p.get("stop"))
            key = (ch_id, start, stop)
            programme_index.setdefault(key, []).append(p)

    for ch in channel_index.values():
        tv.append(ch)

    for key, plist in programme_index.items():
        base = plist[0]
        base.set("start", key[1])
        base.set("stop", key[2])
        def set_tag(tag):
            candidates = []
            for p in plist:
                candidates += p.findall(tag)
            if candidates:
                chosen = pick_lang(candidates)
                for old in base.findall(tag):
                    base.remove(old)
                base.append(chosen)
        for tag in ["title", "sub-title", "desc"]:
            set_tag(tag)
        tv.append(base)

    return etree.ElementTree(tv)

def main():
    sources = load_sources()
    print("Sources loaded:", sources)
    tree = merge_sources(sources)
    print("Merging done, writing epg.xml.gz")
    with gzip.open("epg.xml.gz", "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True)
    print("File epg.xml.gz written successfully!")

if __name__ == "__main__":
    main()