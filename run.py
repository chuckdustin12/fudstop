from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path
from pydub import AudioSegment
from pydub.playback import play
import os
import time
import uuid

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_KEY"))

CHARACTERS = {
    "Donald J. Trump": {
        "voice": "onyx",
        "instructions": "You whisper very softly. Always whisper extremely quietly."
    },
    "Lucy": {
        "voice": "ballad",
        "instructions": "You're very scared and shakey."
    },

}

SCRIPT = [
    ("Donald J. Trump", "Hey, psst. Hey kid. Over here."),
    ("Lucy", "What? Who said tha?"),
    ("Donald J. Trump", "Look this way."),
    ("Lucy", "Get back, I'm warning you, I have a dildo."),
    ("Donald J. Trump", "Do you want some candy little boy?"),

]

OUTPUT_DIR = Path(__file__).parent / "audio_lines"
OUTPUT_DIR.mkdir(exist_ok=True)

def generate_and_play(character, line):
    voice = CHARACTERS[character]["voice"]
    instructions = CHARACTERS[character]["instructions"]
    filename = OUTPUT_DIR / f"{uuid.uuid4()}.mp3"

    with client.audio.speech.with_streaming_response.create(
        model="gpt-4o-mini-tts",
        voice=voice,
        input=line,
        instructions=instructions,
    ) as response:
        response.stream_to_file(filename)

    print(f"{character} says: {line}")
    
    # Use pydub to play the audio
    audio = AudioSegment.from_file(filename)
    play(audio)


def main():
    for character, line in SCRIPT:
        generate_and_play(character, line)

if __name__ == "__main__":
    main()
