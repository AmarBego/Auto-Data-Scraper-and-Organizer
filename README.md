# Auto-Data.net Scraper and Organizer

A Python-based web scraping solution for extracting and organizing car model data from auto-data.net, featuring proxy support and structured JSON output.

## Features
- Sitemap-based URL discovery
- Multi-language support
- Proxy rotation for reliable scraping
- Structured data organization
- JSON specification extraction
- Progress tracking with tqdm

## Prerequisites
- Python 3.8+
- Command line interface
- Valid proxies (minimum 10 required)
- Basic understanding of JSON formatting

## Installation
1. Clone/download the repository
2. Navigate to project directory:
   ```bash
   cd path/to/project
   ```
3. Run the script (automatic & guided setup):
   ```bash
   python scraper.py
   ```

The script will:
- Create a virtual environment
- Install required dependencies
- Set up Scrapy project structure
- Validate configuration files

## Configuration

### 1. Car Brands File (`car_brands.json`)
Create a JSON file with brand information:
```json
[
    {"id": 1, "name": "MERCEDES-BENZ"},
    {"id": 2, "name": "BMW"},
    {"id": 3, "name": "AUDI"}
]
```
- Names must be UPPERCASE with hyphens instead of spaces
- IDs must be unique integers

### 2. Proxies File (`proxies.txt`)
Add at least 10 proxies in IP:PORT format:
```
123.45.67.89:8080
234.56.78.90:3128
...
```

## Usage
1. **First Run Setup**:
   - Select your preferred locale when prompted
   - Add valid proxies to `proxies.txt`

2. **Scraping Execution**:
   ```bash
   python scraper.py
   ```
   The script will:
   - Process sitemaps
   - Scrape car model pages
   - Save HTML as JSON files
   - Organize data into `organized_car_models.json`

3. **Output Files**:
   - `sitemap_links.txt`: All discovered car model URLs
   - `json_files/`: Raw scraped data (HTML converted to JSON)
   - `organized_car_models.json`: Final structured output

## Troubleshooting

### Common Issues
1. **Missing Car Brands**:
   - Ensure `car_brands.json` exists in project root
   - Validate JSON structure using online validators

2. **Proxy Errors**:
   - Verify at least 10 valid proxies in `proxies.txt`
   - Check proxy format: `IP:PORT` without authentication

3. **Path Issues**:
   - Run script from project root directory
   - Ensure virtual environment is created (`.venv` folder)

4. **Data Mismatches**:
   - Check `skipped_brands.log` for processing errors
   - Verify locale matches scraped content language

## Data Structure
Final output (`organized_car_models.json`) contains:
```json
[
  {
    "brand_id": 4,
    "models": [
      {
        "model_id": 1,
        "name": "3 Series E46",
        "submodels": [
          {
            "submodel_id": 1,
            "name": "320d Coupe",
            "variants": [
              {
                "id": 1,
                "specification": "150HP Automatic",
                "link": "https://...",
                "specs": { /* technical data */ }
              }
            ]
          }
        ]
      }
    ]
  }
]
```

## Legal Considerations
- Respect website's `robots.txt` rules
- Use proxies ethically
- Limit request rate
- Check website's terms of service
- Consider caching responses to avoid repeated scraping

> **Note**: Web scraping may be against the terms of service of some websites. Use this tool responsibly and at your own risk.