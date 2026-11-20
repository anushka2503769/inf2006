import os
import requests
import zipfile
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin

url = "https://data.gdeltproject.org/events/"
folder = "gdelt_data"
os.makedirs(folder, exist_ok=True)

session = requests.Session()
soup = BeautifulSoup(session.get(url, timeout=60).text, "html.parser")

for link in soup.find_all("a", href=True):
    filename = link["href"].split("/")[-1]

    if not filename.endswith(".zip") or filename == "GDELT.MASTERREDUCEDV2.1979-2013.zip":
        continue

    file_url = urljoin(url, link["href"])
    zip_path = os.path.join(folder, filename)
    extract_folder = os.path.join(folder, filename[:-4])
    temp_path = zip_path + ".part"

    try:
        # Download with retries and resume partial downloads
        for attempt in range(5):
            try:
                if not os.path.exists(zip_path):
                    downloaded = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0
                    headers = {"Range": f"bytes={downloaded}-"} if downloaded else {}

                    with session.get(
                        file_url, headers=headers, stream=True, timeout=(30, 120)
                    ) as response:
                        response.raise_for_status()

                        # Append only if the server supports resuming
                        mode = "ab" if downloaded and response.status_code == 206 else "wb"

                        with open(temp_path, mode) as f:
                            for chunk in response.iter_content(1024 * 1024):
                                if chunk:
                                    f.write(chunk)

                    os.replace(temp_path, zip_path)

                # Extract only if not already extracted
                if not os.path.isdir(extract_folder):
                    os.makedirs(extract_folder, exist_ok=True)
                    with zipfile.ZipFile(zip_path, "r") as z:
                        z.extractall(extract_folder)

                print("Completed:", filename)
                break

            except (requests.RequestException, zipfile.BadZipFile, OSError) as e:
                print(f"Attempt {attempt + 1}/5 failed for {filename}: {e}")
                if attempt == 4:
                    print("Skipping:", filename)
                else:
                    time.sleep(3)

    except Exception as e:
        print("Error:", filename, e)

print("Finished processing GDELT files.")
