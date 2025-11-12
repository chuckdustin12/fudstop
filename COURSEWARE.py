



import requests


r = requests.get("https://u1sact.webullfinance.com/api/edu/v1/l/lesson/getCoursewareDetail?coursewareId=YiTbPG&courseId=I298sa")

r = r.json()
video_url = r['videos']


# webull_hls_to_mp4.py
import subprocess
import shutil
import requests
from pathlib import Path

WEBULL_API = "https://u1sact.webullfinance.com/api/edu/v1/l/lesson/getCoursewareDetail"


def get_m3u8_from_courseware(courseware_id: str, course_id: str = "I298sa") -> str:
    """Return the .m3u8 URL for a given Webull coursewareId."""


    resp = requests.get(
        WEBULL_API,
        params={"coursewareId": courseware_id, "courseId": course_id},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    m3u8 = data.get("videos")
    if not m3u8:
        raise ValueError("Could not find 'videos' in API response.")
    return m3u8

def hls_to_mp4(m3u8_url: str, out_path: str) -> Path:
    """
    Download/convert HLS (.m3u8) to MP4.
    Tries fast stream-copy first; if that fails, auto-falls back to re-encode.
    Returns the absolute Path to the MP4.
    """
    if shutil.which("ffmpeg") is None:
        raise RuntimeError("ffmpeg not found on PATH. Install ffmpeg first.")

    out_path = Path(out_path).with_suffix(".mp4")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Build shared ffmpeg args
    ff_base = ["ffmpeg", "-y", "-loglevel", "warning", "-hide_banner"]


    # 1) Fast path: stream copy (no quality loss)
    cmd_copy = ff_base + [

        "-i", m3u8_url,
        "-map", "v:0", "-map", "a:0?",
        "-c", "copy",
        "-movflags", "+faststart",
        str(out_path),
    ]
    try:
        subprocess.check_call(cmd_copy)
        return out_path.resolve()
    except subprocess.CalledProcessError:
        # 2) Fallback: re-encode (slower, but robust)
        cmd_re = ff_base + [
            "-headers", header_str,
            "-i", m3u8_url,
            "-map", "v:0", "-map", "a:0?",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
            "-c:a", "aac", "-b:a", "160k",
            "-movflags", "+faststart",
            str(out_path),
        ]
        subprocess.check_call(cmd_re)
        return out_path.resolve()

def download_webull_courseware_as_mp4(courseware_id: str, course_id: str = "I298sa", output_dir: str = ".") -> tuple[str, str]:
    """
    End-to-end helper:
      1) Fetch .m3u8 from Webull for the given courseware_id
      2) Convert to MP4
      3) Return (m3u8_url, mp4_file_uri)
    """
    m3u8_url = get_m3u8_from_courseware(courseware_id, course_id=course_id)
    # Derive a file name from the last part of the .m3u8 URL
    name = Path(m3u8_url.split("?")[0]).stem or f"{courseware_id}"
    out_path = Path(output_dir) / f"{name}.mp4"
    mp4_path = hls_to_mp4(m3u8_url, str(out_path))
    return m3u8_url, mp4_path.as_uri()

# ---------- Example usage (you can run this directly in VS Code) ----------
if __name__ == "__main__":
    # Replace with whatever coursewareId you want
    courseware_id = "YiTbPG"   # your example
    m3u8, mp4_uri = download_webull_courseware_as_mp4(courseware_id)
    print("HLS (.m3u8):", m3u8)
    print("MP4 file URI:", mp4_uri)
