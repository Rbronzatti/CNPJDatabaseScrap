import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import signal
import sys

class DownloadFiles:
    def __init__(self, output_dir='output', max_workers=5):
        """
        Initialize the DownloadFiles class with an output directory and thread pool size.

        :param output_dir: Directory where ZIP files will be downloaded.
        :param max_workers: Maximum number of parallel download threads.
        """
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.stop_event = threading.Event()  # Event to signal threads to stop
        self._create_output_dir()

    def _create_output_dir(self):
        """
        Create the output directory if it doesn't exist.
        """
        if not os.path.exists(self.output_dir):
            try:
                os.makedirs(self.output_dir)
                print(f"Created directory: {self.output_dir}")
            except OSError as e:
                print(f"Error creating directory {self.output_dir}: {e}")
                raise

    def _parse_size(self, size_str):
        """
        Convert human-readable file size to bytes.

        :param size_str: Size string from HTML (e.g., '22K', '360M', '1.4G').
        :return: Size in bytes as an integer.
        """
        size_str = size_str.strip().upper()
        if size_str.endswith('G'):
            return int(float(size_str[:-1]) * 1024**3)
        elif size_str.endswith('M'):
            return int(float(size_str[:-1]) * 1024**2)
        elif size_str.endswith('K'):
            return int(float(size_str[:-1]) * 1024)
        elif size_str == '-' or size_str == '':
            return 0
        else:
            try:
                return int(size_str)
            except ValueError:
                print(f"Unknown size format: '{size_str}'. Assuming size is 0.")
                return 0

    def _download_single_file(self, zip_url, filename, zip_href, remote_size, position):
        """
        Download a single ZIP file with a progress bar.

        :param zip_url: URL of the ZIP file to download.
        :param filename: Local path where the ZIP file will be saved.
        :param zip_href: Name of the ZIP file (used for the progress bar description).
        :param remote_size: Expected size of the ZIP file in bytes.
        :param position: Position index for the tqdm progress bar.
        """
        try:
            with requests.get(zip_url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                block_size = 8192
                progress_bar = tqdm(
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    desc=zip_href,
                    position=position,
                    leave=False
                )
                with open(filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=block_size):
                        if self.stop_event.is_set():
                            # Stop event is set; terminate the download
                            print(f"\nStopping download for {filename}")
                            progress_bar.close()
                            return
                        if chunk:
                            f.write(chunk)
                            progress_bar.update(len(chunk))
                progress_bar.close()
                print(f"Successfully downloaded: {filename}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {zip_url}: {e}")
        except OSError as e:
            print(f"Error saving {filename}: {e}")

    def download_zip_files(self, url):
        """
        Download all ZIP files from the given URL into the output directory,
        verifying file sizes to avoid redundant downloads. Downloads are performed in parallel.

        :param url: URL of the page containing ZIP file links.
        """
        # Define the signal handler within the method
        def signal_handler(sig, frame):
            print("\nInterrupt received! Stopping all downloads gracefully...")
            self.stop_event.set()

        # Register the signal handler
        signal.signal(signal.SIGINT, signal_handler)

        try:
            response = requests.get(url)
            response.raise_for_status()
            print(f"Successfully fetched the page: {url}")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching the URL {url}: {e}")
            raise

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all <a> tags with href ending with .zip (case-insensitive)
        zip_links = soup.find_all('a', href=lambda href: href and href.lower().endswith('.zip'))

        if not zip_links:
            print("No ZIP files found on the page.")
            return

        print(f"Found {len(zip_links)} ZIP file(s) to download.")

        # Prepare a list of download tasks
        download_tasks = []
        for idx, link in enumerate(zip_links):
            zip_href = link.get('href')
            zip_url = urljoin(url, zip_href)
            filename = os.path.join(self.output_dir, zip_href)

            # Extract the size from the corresponding <td>
            tr = link.find_parent('tr')
            size_td = tr.find_all('td')[3] if tr and len(tr.find_all('td')) >= 4 else None
            remote_size_str = size_td.text.strip() if size_td else '-'
            remote_size = self._parse_size(remote_size_str)

            # Check if file exists and compare sizes
            if os.path.exists(filename):
                local_size = os.path.getsize(filename)
                if local_size == remote_size:
                    print(f"File already exists and size matches: {filename}. Skipping download.")
                    continue
                else:
                    print(f"File exists but size differs (local: {local_size} bytes, remote: {remote_size} bytes). Re-downloading.")

            # Assign a unique position for each progress bar
            position = idx % self.max_workers

            # Append the download task details
            download_tasks.append((zip_url, filename, zip_href, remote_size, position))

        if not download_tasks:
            print("No files need to be downloaded.")
            return

        # Initialize the ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all download tasks
            future_to_download = {
                executor.submit(
                    self._download_single_file,
                    task[0],  # zip_url
                    task[1],  # filename
                    task[2],  # zip_href
                    task[3],  # remote_size
                    task[4]   # position
                ): task for task in download_tasks
            }

            try:
                # Handle task completion
                for future in as_completed(future_to_download):
                    task = future_to_download[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"An error occurred during downloading {task[2]}: {e}")
            except KeyboardInterrupt:
                print("\nKeyboardInterrupt received! Initiating shutdown...")
                self.stop_event.set()
                executor.shutdown(wait=False)
                print("All download tasks have been signaled to stop.")

        print("All download tasks have been processed.")