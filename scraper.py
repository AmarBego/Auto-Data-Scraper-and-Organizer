import os
import sys
import subprocess
import venv
import json

def setup_environment():
    """Create virtual environment and install required packages"""
    venv_dir = os.path.join(os.path.dirname(__file__), '.venv')
    python_exec = os.path.join(venv_dir, 'Scripts' if os.name == 'nt' else 'bin', 'python.exe' if os.name == 'nt' else 'python')
    
    if not os.path.exists(venv_dir):
        print("Creating virtual environment...")
        venv.create(venv_dir, with_pip=True)
        print("Virtual environment created at:", venv_dir)
    
    required_packages = [
        'requests',
        'beautifulsoup4', 
        'tqdm',
        'inquirer',
        'lxml',
        'scrapy',
        'scrapy-rotating-proxies'
    ]
    
    try:
        import requests
        from bs4 import BeautifulSoup
        from tqdm import tqdm
        import inquirer
        import scrapy
        import rotating_proxies
    except ImportError:
        print("Installing required packages...")
        subprocess.check_call([python_exec, '-m', 'pip', 'install'] + required_packages)
        print("Packages installed successfully")
    
    # Restart script using virtual env Python
    # Normalize paths for comparison on Windows
    current_exec = os.path.normpath(sys.executable).lower()
    target_exec = os.path.normpath(python_exec).lower()
    
    if current_exec != target_exec:
        print("Restarting with virtual environment...")
        if os.name == 'nt':  # Windows
            subprocess.run([python_exec] + sys.argv)
            sys.exit(0)
        else:  # Unix-like systems
            os.execv(python_exec, [python_exec] + sys.argv)

def check_car_brands():
    """Check if car_brands.json exists and has the correct structure"""
    if not os.path.exists('car_brands.json'):
        print("Error: car_brands.json not found.")
        print("Please create a car_brands.json file with the following structure:")
        print("""
[
    {"id": 1, "name": "MERCEDES-BENZ"},
    {"id": 2, "name": "ROLLS-ROYCE"},
    {"id": 3, "name": "ALPINA"},
    {"id": 4, "name": "BMW"},
    {"id": 5, "name": "AUDI"}
]
""")
        print("Note: All brand names must be in uppercase and use hyphens instead of spaces.")
        sys.exit(1)
    
    try:
        with open('car_brands.json', 'r', encoding='utf-8') as f:
            brands = json.load(f)
            if not isinstance(brands, list) or not all('name' in item for item in brands):
                raise ValueError("Invalid structure")
    except (json.JSONDecodeError, ValueError):
        print("Error: car_brands.json has an invalid structure.")
        print("Please ensure it matches the required format.")
        sys.exit(1)

setup_environment()

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm
import os
import inquirer
import scrapy
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

SITEMAP_URL = "https://www.auto-data.net/sitemap.xml"
OUTPUT_FILE = "sitemap_links.txt"
LOG_FILE = "sitemap.log"
C_PATTERN = "sitemap/c.php"

LOCALES = {
    'en': 'English',
    'bg': 'Bulgarian', 
    'ru': 'Russian',
    'de': 'German',
    'it': 'Italian',
    'fr': 'French',
    'es': 'Spanish',
    'tr': 'Turkish',
    'ro': 'Romanian',
    'gr': 'Greek',
    'fi': 'Finnish',
    'se': 'Swedish',
    'no': 'Norwegian',
    'pl': 'Polish'
}

def get_sitemap_links():
    """Fetch and parse the main sitemap using faster XML parsing"""
    try:
        response = requests.get(SITEMAP_URL, timeout=30)
        response.raise_for_status()
        
        # Use ElementTree for faster XML parsing
        root = ET.fromstring(response.content)
        
        # Handle XML namespaces
        namespaces = {'': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        sitemap_urls = []
        for loc in root.findall('.//loc', namespaces):
            if loc.text and C_PATTERN in loc.text:
                sitemap_urls.append(loc.text)
        
        print(f"Found {len(sitemap_urls)} sitemap URLs")
        return sitemap_urls
        
    except Exception as e:
        print(f"Error fetching main sitemap: {e}")
        # Fallback to BeautifulSoup if ElementTree fails
        response = requests.get(SITEMAP_URL)
        soup = BeautifulSoup(response.content, 'lxml-xml')
        return [loc.text for loc in soup.find_all('loc') if C_PATTERN in loc.text]

def extract_urls_fast(sitemap_url):
    """Extract URLs from a sitemap using faster XML parsing"""
    try:
        response = requests.get(sitemap_url, timeout=30)
        response.raise_for_status()
        
        # Use ElementTree for faster parsing
        root = ET.fromstring(response.content)
        
        # Handle XML namespaces
        namespaces = {'': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        urls = []
        for loc in root.findall('.//loc', namespaces):
            if loc.text:
                urls.append(loc.text)
        
        return urls
        
    except Exception as e:
        print(f"Error parsing {sitemap_url}: {e}")
        return []

def extract_urls(sitemap_url):
    """Extract URLs from a sitemap page - kept for compatibility"""
    return extract_urls_fast(sitemap_url)

def load_existing_links():
    """Load existing links from output file"""
    if not os.path.exists(OUTPUT_FILE):
        return set()
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f)

def save_new_links(new_links):
    """Save new links to file"""
    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        f.writelines(f"{link}\n" for link in sorted(new_links)) 

def log_new_links(count):
    """Log new links added"""
    if count > 0:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{timestamp} - Added {count} new links\n")

def check_locale_file():
    """Check if saved_locale.txt exists and is valid"""
    if os.path.exists('saved_locale.txt'):
        try:
            with open('saved_locale.txt', 'r', encoding='utf-8') as f:
                locale = f.read().strip()
                if locale not in LOCALES:
                    print("Warning: saved_locale.txt contains an invalid locale.")
                    return None
            return locale
        except Exception as e:
            print(f"Warning: Error reading saved_locale.txt - {str(e)}")
            return None
    return None

def save_locale(locale):
    """Save selected locale to file"""
    with open('saved_locale.txt', 'w', encoding='utf-8') as f:
        f.write(locale)

def select_locales():
    """Prompt user to select locales"""
    saved_locale = check_locale_file()
    if saved_locale:
        print(f"Using saved locale: {LOCALES[saved_locale]} ({saved_locale})")
        return [saved_locale]
    
    questions = [
        inquirer.List('locales',
                     message="Select locale to save",
                     choices=[(f"{name} ({code})", code) for code, name in LOCALES.items()],
                    ),
    ]
    answers = inquirer.prompt(questions)
    selected_locale = answers['locales']
    
    save_locale(selected_locale)
    return [selected_locale]

def filter_by_locale_and_brand(urls, locales, brands):
    """Filter URLs by selected locales and car brands"""
    brand_patterns = [
        (brand['name'].lower().replace(' ', '-'), brand['name']) 
        for brand in brands
    ]
    brand_patterns.sort(key=lambda x: len(x[0]), reverse=True)
    
    filtered_urls = []
    for url in urls:
        matched_locale = None
        for locale in locales:
            if url.startswith(f"https://www.auto-data.net/{locale}/"):
                matched_locale = locale
                break
        
        if not matched_locale:
            continue
            
        path_part = url.split(f"www.auto-data.net/{matched_locale}/")[-1].split('/')[0]
        
        for pattern, original_name in brand_patterns:
            if path_part.startswith(pattern + '-') or path_part == pattern:
                filtered_urls.append(url)
                break
                
    return sorted(filtered_urls)
                
def process_sitemap_batch(sitemap_urls, locales, brands, existing_links):
    """Process a batch of sitemaps concurrently"""
    new_links = set()
    
    def process_single_sitemap(sitemap_url):
        try:
            urls = extract_urls_fast(sitemap_url)
            filtered_urls = filter_by_locale_and_brand(urls, locales, brands)
            return [url for url in filtered_urls if url not in existing_links]
        except Exception as e:
            print(f"Error processing {sitemap_url}: {e}")
            return []
    
    # Use ThreadPoolExecutor for concurrent processing
    max_workers = min(100, len(sitemap_urls))  # Limit concurrent requests
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_sitemap = {
            executor.submit(process_single_sitemap, sitemap_url): sitemap_url 
            for sitemap_url in sitemap_urls
        }
        
        # Process completed tasks with progress bar
        with tqdm(total=len(sitemap_urls), desc="Processing sitemaps") as pbar:
            for future in as_completed(future_to_sitemap):
                sitemap_url = future_to_sitemap[future]
                try:
                    urls = future.result()
                    new_links.update(urls)
                    pbar.set_postfix({'New URLs': len(new_links)})
                except Exception as e:
                    print(f"Error processing {sitemap_url}: {e}")
                finally:
                    pbar.update(1)
    
    return new_links

def setup_scrapy_project():
    """Create and configure Scrapy project with proxy support"""
    venv_dir = os.path.join(os.path.dirname(__file__), '.venv')
    python_exec = os.path.join(venv_dir, 'Scripts' if os.name == 'nt' else 'bin', 'python.exe' if os.name == 'nt' else 'python')
    
    project_name = "autodata"
    project_root = os.path.join(os.getcwd(), project_name)
    project_inner = os.path.join(project_root, project_name)
    proxies_path = os.path.join(project_root, "proxies.txt")

    try:
        subprocess.check_call([sys.executable, "-m", "pip", "show", "scrapy"], 
                            stdout=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        print("Installing Scrapy...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "scrapy"])

    if not os.path.exists(project_inner):
        print("Creating Scrapy project structure...")
        subprocess.check_call(["scrapy", "startproject", project_name])

    files_to_update = {
        'middlewares.py': '''
from scrapy import signals
from itemadapter import is_item, ItemAdapter


class AutodataSpiderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        return None

    def process_spider_output(self, response, result, spider):
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        pass

    def process_start_requests(self, start_requests, spider):
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class AutodataDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        return None

    def process_response(self, request, response, spider):
        return response

    def process_exception(self, request, exception, spider):
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
''',
        'pipelines.py': '''
import os
import json
from bs4 import BeautifulSoup
from tqdm import tqdm

class AutodataPipeline:
    def __init__(self):
        self.progress_bar = None
        self.item_count = 0

    def open_spider(self, spider):
        self.progress_bar = tqdm(desc="Processing pages", unit="page")
        os.makedirs('json_files', exist_ok=True)

    def close_spider(self, spider):
        self.progress_bar.close()

    def parse_html_to_dict(self, html_content):
        """Parse HTML content into a dictionary of key-value pairs."""
        soup = BeautifulSoup(html_content, 'html.parser')
        data = {}
        
        for row in soup.find_all('tr'):
            th = row.find('th')
            td = row.find('td')
            if th and td:
                key = th.text.strip()
                value = td.text.strip()
                data[key] = value
        
        if not data:
            for th in soup.find_all('th'):
                td = th.find_next('td')
                if td:
                    key = th.text.strip()
                    value = td.text.strip()
                    data[key] = value
        
        return data

    def process_item(self, item, spider):
        data = self.parse_html_to_dict(item['html_content'])
        
        filename = f"{item['url_slug']}.json"
        filepath = os.path.join('json_files', filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        
        self.item_count += 1
        self.progress_bar.update(1)
        self.progress_bar.set_postfix({'Processed': self.item_count})
        
        return item
''',
        'settings.py': '''
BOT_NAME = 'autodata'

SPIDER_MODULES = ['autodata.spiders']
NEWSPIDER_MODULE = 'autodata.spiders'

ROBOTSTXT_OBEY = False

DOWNLOADER_MIDDLEWARES = {
    'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
    'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
}

ROTATING_PROXY_LIST_PATH = 'proxies.txt'

RETRY_TIMES = 3

DOWNLOAD_DELAY = 1

CONCURRENT_REQUESTS = 100

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

COOKIES_ENABLED = False

TELNETCONSOLE_ENABLED = False

DEFAULT_REQUEST_HEADERS = {
   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
   "Accept-Language": "en",
}

SPIDER_MIDDLEWARES = {
   "autodata.middlewares.AutodataSpiderMiddleware": 543,
}

EXTENSIONS = {
    'scrapy.extensions.logstats.LogStats': None,
    'scrapy.extensions.telnet.TelnetConsole': None,
}

ITEM_PIPELINES = {
    'autodata.pipelines.AutodataPipeline': 300,
}

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 5
AUTOTHROTTLE_MAX_DELAY = 60
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = False

HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 0
HTTPCACHE_DIR = "httpcache"
HTTPCACHE_IGNORE_HTTP_CODES = []
HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

LOG_LEVEL = 'ERROR'

''',
        'spiders/carmodels_spider.py': '''
import scrapy
import os

class AutoDataSpider(scrapy.Spider):
    name = "autodata"
    
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    links_file = os.path.join(project_root, '../sitemap_links.txt')
    
    with open(links_file, encoding='utf-8') as f:
        start_urls = [line.strip() for line in f if line.strip()]

    def parse(self, response):
        slug = response.url.split('/en/')[-1].split('/')[0].split('?')[0]
        
        yield {
            'html_content': response.text,
            'url_slug': slug
        }
'''
    }

    for file_path, content in files_to_update.items():
        full_path = os.path.join(project_inner, file_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content.strip())
        print(f"Updated {file_path}")

    proxies_exist = os.path.exists(proxies_path) and os.path.getsize(proxies_path) > 50

    if not proxies_exist:
        print("\n\033[1;33mACTION REQUIRED:\033[0m")
        print(f"1. Add at least 10 proxies to {os.path.abspath(proxies_path)}")
        print("2. Save the file before continuing\n")

        with open(proxies_path, 'w', encoding='utf-8') as f:
            f.write("# Add proxies here (one per line)\n")
            f.write("# Format: ip:port\n")
            f.write("# Example: 123.45.67.89:8080\n")

        questions = [
            inquirer.Confirm('ready',
                message="Have you added proxies to proxies.txt?",
                default=False)
        ]
        
        if not inquirer.prompt(questions)['ready']:
            print("Aborting: Proxy setup required")
            sys.exit(1)

    with open(proxies_path, 'r', encoding='utf-8') as f:
        proxies = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    import re
    proxy_pattern = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$')
    invalid_proxies = [p for p in proxies if not proxy_pattern.match(p)]
    
    if invalid_proxies:
        print("Error: Invalid proxy format found:")
        for p in invalid_proxies[:3]:
            print(f" - {p}")
        print("Please use IP:PORT format without authentication")
        sys.exit(1)
        
    if len(proxies) < 10:
        print(f"Error: Found only {len(proxies)} valid proxies. Minimum 10 required.")
        sys.exit(1)

    settings_path = os.path.join(project_inner, 'settings.py')
    with open(settings_path, 'a', encoding='utf-8') as f:
        f.write(f"\nPROXY_LIST = {proxies}\n")

    print("\n\033[1;32mConfiguration complete!\033[0m")
    
    print("Starting spider...")
    subprocess.run(f"cd {project_name} && {python_exec} -m scrapy crawl autodata", shell=True, check=True)

    process_and_organize_data()

def process_and_organize_data():
    """Process scraped JSON files and organize into final structure"""
    import re
    import logging
    import glob
    from collections import defaultdict

    logging.basicConfig(
        filename='skipped_brands.log',
        level=logging.INFO,
        format='%(asctime)s - %(message)s'
    )

    with open('car_brands.json', 'r', encoding='utf-8') as f:
        brands = json.load(f)
    
    brand_lookup = defaultdict(list)
    for brand in brands:
        normalized = brand['name'].lower().replace(' ', '-')
        parts = normalized.split('-')
        brand_lookup[tuple(parts)].append(brand)

    output = defaultdict(lambda: {
        'models': defaultdict(lambda: {
            'variants': [],
            'submodels': defaultdict(lambda: {
                'variants': []
            })
        })
    }) 

    model_counter = 1
    submodel_counter = 1
    variant_counter = 1

    with open('sitemap_links.txt', encoding='utf-8') as f:
        total_links = len(f.read().splitlines())

    with open('sitemap_links.txt', encoding='utf-8') as f:
        for link in tqdm(f.read().splitlines(), total=total_links, desc="Processing links"):
            if '/en/bmw-' in link:
                match = re.search(r'/en/bmw-([\w-]+)-\d+$', link)
                if not match:
                    logging.info(f"Skipped BMW link: {link} - No valid pattern match")
                    continue
                parts = match.group(1).split('-')
                
                chassis_code = next((p.upper() for p in parts if re.match(r'^[efg]\d{2,3}$', p, re.I)), None)
                body_type = next((p.title() for p in parts if p in {'convertible', 'coupe', 'hatchback'}), 'Sedan')
                engine = next((p.upper() for p in parts if re.match(r'^\d+[a-z]+$', p)), None)
                power = next((p.upper() for p in parts if 'hp' in p), '')
                transmission = next((p.title() for p in parts if p in {'steptronic', 'dct', 'automatic'}), 'Manual')

                series = parts[0].title().replace('series', ' Series')
                model_name = f"{series} {chassis_code}" if chassis_code else series
                submodel_name = f"{engine} {body_type}" if engine else body_type
                variant_spec = f"{power} {transmission}".strip()
                
                brand_data = output[20]
                model_data = brand_data['models'][model_name]
                model_data['submodels'][submodel_name]['variants'].append({
                    'specification': variant_spec,
                    'link': link
                })
                continue

            if '/en/mercedes-benz-' in link:
                match = re.search(r'/en/mercedes-benz-([\w-]+)-\d+$', link)
                if not match:
                    logging.info(f"Skipped Mercedes link: {link} - No valid pattern match")
                    continue
                parts = match.group(1).split('-')
                
                model_key = None
                submodel_parts = []
                for i, part in enumerate(parts):
                    if re.match(r'^[A-Z]\d{2,3}$', part.upper()) or re.match(r'^\d{3}[a-z]?$', part):
                        model_key = part.upper()
                        submodel_parts = parts[i+1:]
                        break
                if not model_key:
                    model_key = parts[0].upper()
                    submodel_parts = parts[1:]
                
                submodel_name = []
                variant_details = []
                for part in submodel_parts:
                    if re.match(r'^\d{3}$', part) or part.lower() in ['cdi', 'td', 'd', 'e', 'se', 'sel', 'amg']:
                        submodel_name.append(part.upper())
                    else:
                        variant_details.append(part.title())
                
                submodel_id = '-'.join(submodel_name) if submodel_name else 'base'
                brand_data = output[91]
                model_data = brand_data['models'][model_key]
                model_data['submodels'][submodel_id]['variants'].append({
                    'specification': ' '.join(variant_details),
                    'link': link
                })
                continue

            path_match = re.search(r'/en/(.+?)(?:-\d+)?$', link)
            if not path_match:
                logging.info(f"Skipped link: {link} - No valid path match")
                continue
                
            full_path = path_match.group(1)
            parts = full_path.split('-')
            
            max_length = 0
            selected_brand = None
            for i in range(1, len(parts)+1):
                brand_parts = tuple(parts[:i])
                if brand_parts in brand_lookup:
                    max_length = i
                    selected_brand = brand_lookup[brand_parts][0]
                    
            if not selected_brand:
                logging.info(f"Skipped link: {link} - No matching brand found")
                continue
                
            if max_length >= len(parts):
                logging.info(f"Skipped link: {link} - No model information after brand")
                continue
                
            model_name = parts[max_length].title()
            variant = ' '.join(parts[max_length+1:]).title()

            brand_data = output[selected_brand['id']]
            brand_data['models'][model_name]['variants'].append({
                'specification': variant,
                'link': link
            })

    def safe_extract_metric(value):
        if not value:
            return None
        try:
            metric_match = re.search(r'([\d.,]+)\s*(?:km/h|kg|mm|m|l|g|cm³|kW|Nm|Hp|hp|m/s²|cd|m³|rpm|°C|g/km|m\-)', str(value), re.IGNORECASE)
            if metric_match:
                return float(metric_match.group(1).replace(',', ''))
            number_match = re.search(r'(\d+[\.,]?\d*)', str(value))
            return float(number_match.group().replace(',', '')) if number_match else None
        except:
            return None

    def safe_extract_int(value):
        if not value:
            return None
        try:
            return int(re.search(r'\d+', str(value)).group())
        except:
            return None

    def process_json_file(file_path):
        try:
            with open(file_path, encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}")
            return None

        base_name = os.path.basename(file_path)
        url_slug = os.path.splitext(base_name)[0]
        
        specs = {
            'source_link': f"https://www.auto-data.net/en/{url_slug}",
            'specifications': {
                'generation': data.get('Generation'),
                'production': {
                    'start': data.get('Start of production'),
                    'end': data.get('End of production')
                },
                'body': {
                    'type': data.get('Body type'),
                    'seats': safe_extract_int(data.get('Seats')),
                    'doors': safe_extract_int(data.get('Doors'))
                },
                'performance': {
                    'acceleration_0_100': safe_extract_metric(data.get('Acceleration 0 - 100 km/h')),
                    'top_speed': safe_extract_metric(data.get('Maximum speed')),
                    'emission_standard': data.get('Emission standard'),
                    'drag_coefficient': safe_extract_metric(data.get('Drag coefficient (Cd)'))
                },
                'engine': {
                    'type': data.get('Fuel Type'),
                    'power': safe_extract_metric(data.get('Power')),
                    'torque': safe_extract_metric(data.get('Torque')),
                    'displacement': safe_extract_metric(data.get('Engine displacement')),
                    'cylinders': safe_extract_int(data.get('Number of cylinders')),
                    'configuration': data.get('Engine configuration'),
                    'aspiration': data.get('Engine aspiration'),
                    'bore': safe_extract_metric(data.get('Cylinder Bore')),
                    'stroke': safe_extract_metric(data.get('Piston Stroke')),
                    'compression': data.get('Compression ratio'),
                    'valves': safe_extract_int(data.get('Number of valves per cylinder'))
                },
                'dimensions': {
                    'length': safe_extract_metric(data.get('Length')),
                    'width': safe_extract_metric(data.get('Width')),
                    'height': safe_extract_metric(data.get('Height')),
                    'wheelbase': safe_extract_metric(data.get('Wheelbase')),
                    'front_track': safe_extract_metric(data.get('Front track')),
                    'rear_track': safe_extract_metric(data.get('Rear (Back) track'))
                },
                'weight': {
                    'kerb': safe_extract_metric(data.get('Kerb Weight')),
                    'max': safe_extract_metric(data.get('Max. weight')),
                    'load': safe_extract_metric(data.get('Max load'))
                },
                'fuel': {
                    'consumption': {
                        'urban': safe_extract_metric(data.get('Fuel consumption (economy) - urban')),
                        'extra_urban': safe_extract_metric(data.get('Fuel consumption (economy) - extra urban')),
                        'combined': safe_extract_metric(data.get('Fuel consumption (economy) - combined'))
                    },
                    'capacity': safe_extract_metric(data.get('Fuel tank capacity'))
                },
                'transmission': {
                    'type': data.get('Drive wheel'),
                    'gears': safe_extract_int(data.get('Number of gears and type of gearbox')),
                    'gearbox': data.get('Number of gears and type of gearbox')
                },
                'suspension': {
                    'front': data.get('Front suspension'),
                    'rear': data.get('Rear suspension')
                },
                'brakes': {
                    'front': data.get('Front brakes'),
                    'rear': data.get('Rear brakes')
                },
                'tires': {
                    'size': data.get('Tires size'),
                    'rims': data.get('Wheel rims size')
                }
            }
        }
        
        if 'CO2 emissions' in data:
            specs['specifications']['emissions'] = {
                'co2': safe_extract_metric(data['CO2 emissions'])
            }
        
        return specs

    def add_json_specs(output):
        project_root = os.path.join(os.path.dirname(__file__), 'autodata')
        json_path = os.path.join(project_root, 'json_files', '*.json')
        json_files = glob.glob(json_path)
                
        link_map = {}
        for json_file in tqdm(json_files, desc="Processing JSON files"):
            specs = process_json_file(json_file)
            if specs and specs.get('source_link'):
                normalized = re.sub(r'[#?].*$', '', specs['source_link'].lower().strip())
                link_map[normalized] = specs['specifications']

        matched = 0
        total = 0
        
        for brand in output.values():
            for model in brand['models'].values():
                for submodel in model['submodels'].values():
                    for variant in submodel['variants']:
                        total += 1
                        variant_url = re.sub(r'[#?].*$', '', variant['link'].lower().strip())
                        if variant_url in link_map:
                            variant['specs'] = link_map[variant_url]
                            matched += 1
                
                for variant in model['variants']:
                    total += 1
                    variant_url = re.sub(r'[#?].*$', '', variant['link'].lower().strip())
                    if variant_url in link_map:
                        variant['specs'] = link_map[variant_url]
                        matched += 1

        logging.info(f"Matched {matched}/{total} variants ({matched/total:.1%})")

    add_json_specs(output)

    final_output = []
    
    model_counter = 1
    submodel_counter = 1
    variant_counter = 1
    
    for brand_id, data in output.items():
        brand_entry = {
            'brand_id': brand_id,
            'models': []
        }
        
        for model_name, model_data in data['models'].items():
            model_entry = {
                'model_id': model_counter,
                'name': model_name,
            }
            model_counter += 1
            
            if model_data['submodels']:
                model_entry['submodels'] = []
                for sub_name, sub_data in model_data['submodels'].items():
                    submodel_entry = {
                        'submodel_id': submodel_counter,
                        'name': sub_name.replace('-', ' ') if '-' in sub_name else sub_name,
                        'variants': [{
                            'id': variant_counter + idx,
                            'specification': var['specification'],
                            'link': var['link'],
                            'specs': var.get('specs', {})
                        } for idx, var in enumerate(sub_data['variants'])]
                    }
                    variant_counter += len(sub_data['variants'])
                    submodel_counter += 1
                    model_entry['submodels'].append(submodel_entry)
            else:
                model_entry['variants'] = [{
                    'variant_id': variant_counter + idx,
                    'specs': var.get('specs', {})
                } for idx, var in enumerate(model_data['variants'])]
                variant_counter += len(model_data['variants'])
            
            brand_entry['models'].append(model_entry)
        
        final_output.append(brand_entry)

    with open('organized_car_models.json', 'w', encoding='utf-8') as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)
    print("Data organization complete! Output saved to organized_car_models.json")


def main():
    check_car_brands()
    
    with open('car_brands.json', 'r', encoding='utf-8') as f:
        brands = json.load(f)
    
    selected_locales = select_locales()
    if not selected_locales:
        print("No locales selected. Exiting.")
        return
    
    print("Fetching sitemap index...")
    c_sitemaps = get_sitemap_links()
    
    print("Loading existing links...")
    existing_links = load_existing_links()
    
    print(f"Processing {len(c_sitemaps)} sitemaps concurrently...")
    start_time = time.time()
    
    # Process all sitemaps concurrently
    new_links = process_sitemap_batch(c_sitemaps, selected_locales, brands, existing_links)
    
    end_time = time.time()
    print(f"Processing completed in {end_time - start_time:.2f} seconds")
    
    if new_links:
        save_new_links(new_links)
        log_new_links(len(new_links))
        print(f"Added {len(new_links)} new links")
    else:
        print("No new links found for selected locales")

    setup_scrapy_project() 
if __name__ == "__main__":
    main()