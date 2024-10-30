import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
from datetime import datetime

class ExtractUrl:
    def __init__(self, base_url):
        """
        Initialize the ExtractTable with the base URL.

        :param base_url: The main URL to scrape.
        """
        self.base_url = base_url

    def get_latest_month_url(self):
        """
        Fetch the main page, parse the HTML, and find the latest month's URL.

        :return: Full URL to the latest month's directory.
        :raises ValueError: If no valid month links are found.
        """
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()  # Raise an exception for HTTP errors
        except requests.exceptions.RequestException as e:
            print(f"Error fetching the main URL: {e}")
            raise

        soup = BeautifulSoup(response.text, 'html.parser')

        # Regular expression to match hrefs like 'yyyy-mm/'
        month_pattern = re.compile(r'^\d{4}-\d{2}/$')
        month_links = soup.find_all('a', href=month_pattern)

        if not month_links:
            raise ValueError("No month links found on the page.")

        # Extract year and month from hrefs and store with the link
        months = []
        for link in month_links:
            href = link.get('href')
            match = re.match(r'^(\d{4})-(\d{2})/$', href)
            if match:
                year, month = int(match.group(1)), int(match.group(2))
                months.append((year, month, href))

        if not months:
            raise ValueError("No valid month links found after parsing.")

        # Sort the months to find the latest one
        latest = max(months, key=lambda x: (x[0], x[1]))
        latest_year, latest_month, latest_href = latest

        # Construct the full URL for the latest month
        latest_month_url = urljoin(self.base_url, latest_href)
        return latest_month_url

    def fetch_latest_month_page(self):
        """
        Fetch the HTML content of the latest month's page.

        :return: HTML content of the latest month's directory.
        """
        latest_url = self.get_latest_month_url()
        try:
            response = requests.get(latest_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching the latest month's URL: {e}")
            raise

        return response.text