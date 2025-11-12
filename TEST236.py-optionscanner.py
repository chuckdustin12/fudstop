import requests
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
import time
opts = PolygonOptions()
import asyncio
import pandas as pd
# Retry wrapper for robust POST calls
def post_with_retry(url, json_data, retries=3, backoff=1.5):
    for attempt in range(retries):
        try:
            response = requests.post(url, json=json_data, timeout=10)
            if response.ok:
                return response.json()
        except Exception as e:
            print(f"[Retry {attempt+1}] Failed to fetch {json_data['partialTag']}: {e}")
            time.sleep(backoff ** attempt)
    return None


# Full 100 partial tags
partial_tags = [
    "uplifting", "dreamy", "dark", "gritty", "funky", "jazzy", "bluesy", "psychedelic", "groovy", "ethereal",
    "moody", "romantic", "euphoric", "chill", "hypnotic", "glitchy", "industrial", "lofi", "retro", "nostalgic",
    "experimental", "glitch", "cinematic", "thrilling", "tense", "ominous", "warm", "cold", "crunchy", "saturated",
    "spacious", "minimal", "introspective", "explosive", "anthemic", "repetitive", "driving", "soulful", "aggressive",
    "vibrant", "suspenseful", "high-energy", "low-key", "punchy", "bouncy", "distorted", "smooth", "percussive", "fluid",
    "detuned", "layered", "harmonic", "glossy", "vintage", "sci-fi", "cinematic rise", "drop", "bassy", "wavy",
    "plucky", "trippy", "tropical", "grotesque", "atmospheric", "metallic", "mechanical", "dynamic", "lush", "loopy",
    "reverberant", "dissonant", "glassy", "sparse", "orchestral", "symphonic", "earthy", "organic", "techy", "noisy",
    "rising", "falling", "fragmented", "muted", "chaotic", "clean", "weird", "wet", "dry", "analog",
    "resonant", "filtered", "compressive", "swelling", "cinematic bass", "electro", "tape", "vocoder", "choral", "epic"
]

async def main():
    await opts.connect()

    for ptag in partial_tags:
        data = {"partialTag": ptag, "currentTags": [""]}
        response = post_with_retry("https://www.udio.com/api/searchtags", data)

        if response and 'completions' in response:
            completions = response.get('completions', [])
            labels = [i.get('label') for i in completions]

            df = pd.DataFrame({
                'ptag': [ptag] * len(labels),
                'label': labels
            })

            await opts.batch_upsert_dataframe(df, table_name='udio_labels', unique_columns=['ptag', 'label'])
        else:
            print(f"[ERROR] Skipping: {ptag} — no completions returned.")

asyncio.run(main())