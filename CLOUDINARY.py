import cloudinary
import cloudinary.uploader
from cloudinary.utils import cloudinary_url
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Cloudinary config
cloudinary.config(
    cloud_name="dfwahwhwl",
    api_key=os.environ.get("CLOUDINARY_KEY"),
    api_secret=os.environ.get("CLOUDINARY_SECRET"),
    secure=True
)

# Path to your directory
folder_path = r"C:\Users\chuck\OneDrive\ANNA_KENDRICK"

# Loop through files in directory
for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    if os.path.isfile(file_path):  # make sure it's a file
        # Create a public_id without extension
        public_id = os.path.splitext(filename)[0]

        try:
            upload_result = cloudinary.uploader.upload(
                file_path,
                public_id=public_id,
                overwrite=True
            )
            print(f"Uploaded {filename}: {upload_result['secure_url']}")

            # Generate optimized & cropped variants if you want
            optimize_url, _ = cloudinary_url(public_id, fetch_format="auto", quality="auto")
            auto_crop_url, _ = cloudinary_url(public_id, width=500, height=500, crop="auto", gravity="auto")

            print("   Optimized:", optimize_url)
            print("   Auto-cropped:", auto_crop_url)
        except Exception as e:
            print(f"Error uploading {filename}: {e}")
