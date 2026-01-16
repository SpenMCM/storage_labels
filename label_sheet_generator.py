import time
import re
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
from pywinauto import Application
from tool_base import Tool
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import requests
import urllib3
import os
import sys
import io
import pandas as pd
import openpyxl
import pymupdf
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Handle stdout encoding
try:
    if hasattr(sys.stdout, 'buffer'):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
except (AttributeError, ValueError):
    pass

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration Constants
PAGE_LOAD_TIMEOUT = 60
IMPLICIT_WAIT = 10
CHROMEDRIVER_PATH = r'\\int\mcm\SoftwareLibrary\Chrome Drivers\chromedriver.exe'

# Threading Configuration
MAX_THREADS_LABELS = 10  # For label generation (QR codes + image processing)

# Embedded SVG with inline styles (PyMuPDF doesn't handle CSS classes)
MCMASTER_LOGO_SVG = '''<?xml version="1.0" encoding="utf-8"?>
<svg version="1.1" xmlns="http://www.w3.org/2000/svg" x="0px" y="0px" viewBox="0 0 456.5 65.7">
<g>
    <g>
        <path fill="#346734" d="M41.3,62.5l-1.2-37H40c-0.6,4.3-1,8.6-1.6,12.9l-3.6,24.1H24.4l-4.4-37h-0.1c-0.1,4.4,0,8.7-0.2,13l-1.1,23.9
            H4.9l5.5-58.8h15.8L29.9,35H30c0.4-3.7,0.7-7.3,1.2-10.9l3.1-20.4H50l4.6,58.8H41.3z"/>
        <path fill="#346734" d="M72.9,63.1c-1.1,0.1-2.2,0.3-3.3,0.3c-9.5,0-14.8-10.6-14.8-21.6c0-11.3,6.1-21.3,15.5-21.3
            c0.9,0,1.7,0.1,2.6,0.2V34c-3.6,0.2-5.5,3.1-5.5,7.5c0,3.3,1.6,7.6,4.7,7.6h0.8V63.1z"/>
        <path fill="#346734" d="M110.6,62.5l-1.2-37h-0.1c-0.6,4.3-1,8.6-1.6,12.9l-3.6,24.1H93.7l-4.4-37h-0.1c-0.1,4.4,0,8.7-0.2,13
            l-1.1,23.9H74.2l5.5-58.8h15.8L99.2,35h0.1c0.4-3.7,0.7-7.3,1.2-10.9l3.1-20.4h15.7l4.6,58.8H110.6z"/>
    </g>
    <g>
        <path fill="#346734" d="M280,44.1V31.3h17v12.8H280z"/>
    </g>
    <g>
        <path fill="#346734" d="M149.4,62.6l-0.9-7.6h-8.5l-1.1,7.6h-13.1L137,3.8h15.5l10,58.8H149.4z M147,42.6l-1.4-13.7
            c-0.4-3.7-0.6-7.4-0.9-11.1h-0.1c-0.4,3.7-0.7,7.4-1.2,11.1l-1.7,13.7H147z"/>
        <path fill="#346734" d="M163.7,42.8c1.7,3.3,4.8,6.6,8,6.6c1.7,0,3.7-1.3,3.7-3.9c0-1.4-0.4-2.4-1-3.3c-0.6-0.9-1.3-1.6-2.1-2.3
            c-2.5-2.4-4.7-4.7-6.3-7.5c-1.6-2.7-2.6-6-2.6-10.3c0-6.9,3.5-19.3,14.4-19.3c3.1,0,6.4,1.2,9,3v17.9c-1.5-3-4.6-6.3-7.5-6.3
            c-1.5,0-3.3,1.3-3.3,3.6c0,1.2,0.6,2.3,1.2,3.3c0.7,0.9,1.6,1.7,2.3,2.4c2.7,2.3,4.8,4.5,6.2,7.2c1.5,2.7,2.3,5.8,2.3,10.2
            c0,10.9-6.1,19.7-15,19.7c-3.2,0-6.6-0.9-9.5-2.4V42.8z"/>
        <path fill="#346734" d="M197.1,62.6V18.2h-7V3.8h27v14.4h-7.3v44.5H197.1z"/>
        <path fill="#346734" d="M220.4,3.8H244v13.9h-10.9v8.2h9.8v13.9h-9.8v9H244v13.9h-23.6V3.8z"/>
    </g>
    <g>
        <path fill="#346734" d="M267.7,62.6l-7.1-24.5l-0.1,0.2c0.1,2.9,0.2,5.8,0.2,8.7v15.7h-12.7V3.8h12.3c11,0,19,4.1,19,19.6
            c0,6.5-2.2,12.2-7.3,14.7l9.5,24.5H267.7z M261.5,31c3.4,0,5.2-3.7,5.2-7.5c0-5.1-2.5-7.3-6-7.1V31L261.5,31z"/>
    </g>
    <g>
        <path fill="#346734" d="M325.1,21.1c-1.8-2.2-3.3-3.4-5.9-3.4c-6.6,0-8.2,9.1-8.2,15.6c0,6.9,1.6,15.6,8,15.6c2.9,0,4.7-1.4,6.7-3.7
            l-0.7,16.5c-2.6,1.4-4.9,2.1-7.7,2.1c-10.4,0-19.3-9.9-19.3-30c0-26.4,14.5-30.8,20.7-30.8c2.2,0,4.4,0.4,6.5,1.4V21.1z"/>
        <path fill="#346734" d="M350.9,62.6l-0.9-7.6h-8.5l-1.1,7.6h-13.1l11.3-58.8h15.5l10,58.8H350.9z M348.5,42.6l-1.4-13.7
            c-0.4-3.7-0.6-7.4-0.9-11.1h-0.1c-0.4,3.7-0.7,7.4-1.2,11.1l-1.7,13.7H348.5z"/>
        <path fill="#346734" d="M386,62.6l-7.1-24.5l-0.1,0.2c0.1,2.9,0.2,5.8,0.2,8.7v15.7h-12.7V3.8h12.3c11,0,19,4.1,19,19.6
            c0,6.5-2.2,12.2-7.3,14.7l9.5,24.5H386z M379.8,31c3.4,0,5.2-3.7,5.2-7.5c0-5.1-2.5-7.3-6-7.1V31L379.8,31z"/>
        <path fill="#346734" d="M421.6,62.6l-7.1-24.5l-0.1,0.2c0.1,2.9,0.2,5.8,0.2,8.7v15.7h-12.7V3.8h12.3c11,0,19,4.1,19,19.6
            c0,6.5-2.2,12.2-7.3,14.7l9.5,24.5H421.6z M415.4,31c3.4,0,5.2-3.7,5.2-7.5c0-5.1-2.5-7.3-6-7.1V31L415.4,31z"/>
        <path fill="#346734" d="M454.2,56.6c0,3.4-2.5,6.2-6.3,6.2c-4.4,0-6.3-3.3-6.3-6.2c0-2.9,1.8-6.2,6.3-6.2
            C452,50.4,454.2,53.4,454.2,56.6z M443.2,56.6c0,2.8,1.9,4.7,4.7,4.7c2.8,0,4.7-2,4.7-4.8c0-2.8-1.9-4.7-4.7-4.7
            C445.1,51.9,443.2,53.8,443.2,56.6z M446.6,60H445v-6.8h3.8c1.4,0,2.3,0.6,2.3,2.1c0,1.3-0.7,1.8-1.8,1.8l1.6,2.8h-1.8l-1.4-2.7
            h-1.1V60z M446.6,54.5v1.6h1.9c0.7,0,1-0.2,1-0.8c0-0.8-0.5-0.9-1.4-0.9H446.6z"/>
    </g>
</g>
</svg>'''
MAX_PAGE_RETRIES = 3
MAX_ORDER_HISTORY_ATTEMPTS = 8

# Label size configurations
LABEL_CONFIGS = {
    "small": {
        "label_width_inches": 0.925,
        "label_height_inches": 2.75,
        "labels_per_row": 8,
        "labels_per_column": 3,
        "left_margin_inches": 0.265625,
        "top_margin_inches": 1.375,
        "gap_inches": 0.07588571429,
        "rotate": True,
        "dpi": 600,
        "pdf_dimension_str": "small_.93x2.75",
    },
    "medium": {
        "label_width_inches": 3.75,
        "label_height_inches": 1.5,
        "labels_per_row": 2,
        "labels_per_column": 6,
        "left_margin_inches": 0.375,
        "top_margin_inches": 0.8125,
        "gap_inches": 0.0625,
        "rotate": False,
        "dpi": 600,
        "pdf_dimension_str": "medium_1.5x3.75",
    },
}


class CustomerPartsAnalyzerAndLabelGenerator(Tool):
    """
    Combined tool that analyzes customer order history to find frequently ordered parts
    and automatically generates labels for those parts.
    
    Uses hybrid approach:
    - Sequential Selenium scraping (reliable, avoids WAF)
    - Threaded label generation (fast QR codes + image processing)
    """
    
    name = "customer_parts_analyzer_and_label_generator"
    description = "Analyzes customer order history for frequently ordered parts and generates labels with QR codes."
    
    param_spec = {
        "account_number": {
            "type": "string",
            "description": "Customer account number(s) to analyze. Can be a single account or comma-separated list.",
            "required": False
        },
        "lookback_days": {
            "type": "integer",
            "description": "Number of days to look back in order history (default: 365). Only used with account_number.",
            "required": False
        },
        "min_order_count": {
            "type": "integer",
            "description": "Minimum number of times a part must be ordered to be included (default: 2). Only used with account_number.",
            "required": False
        },
        "excel_file_path": {
            "type": "string",
            "description": "Path to Excel file containing part numbers in column A and optional customer references in column B.",
            "required": False
        },
        "output_folder": {
            "type": "string",
            "description": "Output folder path where labels and analysis will be saved",
            "required": True
        },
        "label_size": {
            "type": "string",
            "description": "Label size: 'small' (0.925x2.75 in), 'medium' (1.5x3.75 in), or 'all' (generates all sizes). Default: 'small'",
            "required": True
        }
    }
    
    def __init__(self):
        self.driver = None
        self.chromedriver_path = CHROMEDRIVER_PATH
        self.cutoff_date = None
        self.mcmaster_logo = None
        self.label_size = "small"
        self.min_order_count = 2
        self.failed_parts = []
        self.hit_date_cutoff = False
        self.run_stats = {}
        self.customer_refs = {}
        self.part_info_map = {}
        self.all_part_counts = {}  # Store unfiltered counts for distribution report
        # Thread-local storage for QR API sessions
        self._thread_local = threading.local()
    
    def _log(self, message: str, level: str = "INFO"):
        """Thread-safe logging"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] [{level}] {message}")
    
    def _reset_stats(self):
        """Reset stats for a new scraping attempt."""
        self.run_stats = {
            "total_order_tiles": 0,
            "orders_processed": 0,
            "orders_in_range": 0,
            "orders_outside_range": 0,
            "orders_no_date": 0,
            "orders_date_parse_fail": 0,
            "total_parts_extracted": 0,
            "component_parts_found": 0,
            "scroll_count": 0,
            "final_scroll_height": 0,
            "final_tile_count": 0,
            "page_reload_attempts": self.run_stats.get("page_reload_attempts", 0),
        }
        self.hit_date_cutoff = False
    
    def _get_thread_session(self) -> requests.Session:
        """Get or create a requests session for the current thread (for QR API calls)."""
        if not hasattr(self._thread_local, 'session'):
            self._thread_local.session = requests.Session()
        return self._thread_local.session
    
    # ==================== EXCEL INPUT METHODS ====================
    
    def _read_part_numbers_from_excel(self, excel_path: str) -> List[str]:
        """Read part numbers from column A and optional customer references from column B."""
        self._log(f"Reading part numbers from Excel file: {excel_path}")
        
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel file not found: {excel_path}")
        
        try:
            df = pd.read_excel(excel_path, header=None, usecols=[0])
            
            if df.empty:
                raise ValueError("Excel file is empty")
            
            has_customer_refs = False
            col_b_data = {}
            try:
                df_with_b = pd.read_excel(excel_path, header=None, usecols=[0, 1])
                if len(df_with_b.columns) > 1:
                    col_b = df_with_b[1]
                    has_customer_refs = col_b.notna().any()
                    if has_customer_refs:
                        for idx, row in df_with_b.iterrows():
                            if pd.notna(row[1]):
                                col_b_data[idx] = str(row[1]).strip()
            except Exception:
                pass
            
            if has_customer_refs:
                self._log("Column B detected - reading customer references")
            else:
                self._log("No column B values - standard mode")
            
            part_numbers = []
            self.customer_refs = {}
            
            for idx, row in df.iterrows():
                part_value = row[0]
                if pd.notna(part_value):
                    part_num = str(part_value).strip().upper()
                    if part_num and part_num.lower() not in ['nan', 'none', '']:
                        part_numbers.append(part_num)
                        
                        if idx in col_b_data:
                            customer_ref = col_b_data[idx]
                            if customer_ref and customer_ref.lower() not in ['nan', 'none', '']:
                                self.customer_refs[part_num] = customer_ref
            
            seen = set()
            unique_parts = []
            for part in part_numbers:
                if part not in seen:
                    seen.add(part)
                    unique_parts.append(part)
            
            self._log(f"Read {len(part_numbers)} part numbers ({len(unique_parts)} unique) from Excel")
            if self.customer_refs:
                self._log(f"Found {len(self.customer_refs)} customer references in column B")
            
            if not unique_parts:
                raise ValueError("No valid part numbers found in Excel file")
            
            for part in unique_parts:
                self.part_info_map[part] = {
                    'display_part': part,
                    'base_part': part,
                    'url_path': part,
                    'is_component': False
                }
            
            return unique_parts
            
        except Exception as e:
            self._log(f"Error reading Excel file: {e}", "ERROR")
            raise
    
    # ==================== LOGO LOADING METHODS ====================
    
    def _load_mcmaster_logo(self) -> Optional[Image.Image]:
        """Load McMaster-Carr logo from embedded SVG using PyMuPDF."""
        if self.mcmaster_logo:
            return self.mcmaster_logo
        
        try:
            doc = pymupdf.open(stream=MCMASTER_LOGO_SVG.encode('utf-8'), filetype="svg")
            page = doc[0]
            matrix = pymupdf.Matrix(3.0, 3.0)
            pixmap = page.get_pixmap(matrix=matrix, alpha=False)
            img = Image.frombytes("RGB", [pixmap.width, pixmap.height], pixmap.samples)
            doc.close()
            self.mcmaster_logo = img
            return img
        except Exception as e:
            self._log(f"Could not load SVG logo: {e}", "ERROR")
            return None
    
    # ==================== WEBDRIVER METHODS ====================
    
    def _create_webdriver(self, headless: bool = False):
        """Create and configure Chrome WebDriver"""
        self._log(f"Setting up Chrome driver (headless={headless})...")
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-popup-blocking")
        service = Service(self.chromedriver_path)
        
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        driver.implicitly_wait(IMPLICIT_WAIT)
        
        self._log("Chrome driver initialized successfully")
        return driver
    
    def _verify_sales_workstation_loaded(self) -> bool:
        """Verify that the Sales Workstation page structure loaded correctly."""
        self._log(f"Verifying Sales Workstation page structure (max {MAX_PAGE_RETRIES} attempts)...")
        
        for attempt in range(1, MAX_PAGE_RETRIES + 1):
            try:
                self.driver.switch_to.window(self.driver.current_window_handle)
                WebDriverWait(self.driver, 60).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                time.sleep(0.3)
                
                if attempt == 1:
                    ActionChains(self.driver).send_keys(Keys.F5).perform()
                    time.sleep(0.5)
                    WebDriverWait(self.driver, 60).until(
                        lambda d: d.execute_script("return document.readyState") == "complete"
                    )
                
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                main_content = soup.find('div', id='MainContent')
                
                if main_content:
                    self._log("Sales Workstation page structure detected")
                    return True
                
                if attempt < MAX_PAGE_RETRIES:
                    self.driver.refresh()
                    time.sleep(0.5)
                    
            except Exception as e:
                self._log(f"Error on attempt {attempt}: {e}", "ERROR")
                if attempt < MAX_PAGE_RETRIES:
                    time.sleep(0.5)
        
        return False
    
    def _navigate_mainframe_icma(self, account_number: str) -> bool:
        """Navigate to mainframe ICMA screen for the given account number."""
        self._log(f"Connecting to mainframe and navigating to ICMA for account {account_number}...")
        try:
            app = Application(backend="win32").connect(title_re="Mainframe.*Rumba.*Desktop")
            window = app.top_window()
            window.set_focus()
            
            window.type_keys("{ESC}")
            time.sleep(0.2)
            window.type_keys("{ESC}")
            time.sleep(0.3)
            
            window.type_keys(f"ICMA{account_number}", with_spaces=False)
            time.sleep(0.3)
            window.type_keys("+{ENTER}")
            time.sleep(0.5)
            window.type_keys("{HOME}")
            time.sleep(0.2)
            window.type_keys("{TAB}")
            time.sleep(0.2)
            window.type_keys("sw", with_spaces=False)
            time.sleep(0.2)
            window.type_keys("+{ENTER}")
            time.sleep(0.5)
            
            self._log("Successfully navigated mainframe ICMA screen")
            return True
        except Exception as e:
            self._log(f"Could not connect to mainframe: {e}", "WARN")
            return False
    
    # ==================== ORDER HISTORY SCRAPING ====================
    
    def _parse_order_date_from_text(self, date_text: str) -> Optional[datetime]:
        """Parse order date from the placed-order-summary-ts text."""
        try:
            date_text = date_text.strip().lower()
            
            if "today" in date_text:
                return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            if "yesterday" in date_text:
                return (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            
            weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            for i, weekday in enumerate(weekdays):
                if weekday in date_text:
                    today = datetime.now()
                    today_weekday = today.weekday()
                    days_back = (today_weekday - i) % 7
                    if days_back == 0:
                        days_back = 7
                    target_date = today - timedelta(days=days_back)
                    return target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            
            match = re.search(r'on\s+(\w+)\s+(\d+)(?:,?\s+(\d{4}))?', date_text)
            if match:
                month_name = match.group(1)
                day = int(match.group(2))
                year_str = match.group(3)
                
                month_map = {
                    'january': 1, 'february': 2, 'march': 3, 'april': 4,
                    'may': 5, 'june': 6, 'july': 7, 'august': 8,
                    'september': 9, 'october': 10, 'november': 11, 'december': 12,
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                    'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9,
                    'oct': 10, 'nov': 11, 'dec': 12
                }
                
                month = month_map.get(month_name.lower())
                if not month:
                    return None
                
                if year_str:
                    year = int(year_str)
                else:
                    current_year = datetime.now().year
                    current_month = datetime.now().month
                    year = current_year if month <= current_month else current_year - 1
                
                try:
                    return datetime(year, month, day, 0, 0, 0)
                except ValueError:
                    return None
        except Exception:
            pass
        return None
    
    def _get_scrollable_element(self):
        """Find and return the scrollable element."""
        try:
            return self.driver.find_element(By.ID, "ActivitySummary")
        except:
            try:
                return self.driver.find_element(By.CSS_SELECTOR, "div.act-summary-cntnr")
            except:
                return None
    
    def _count_order_tiles(self) -> int:
        """Count the current number of order tiles in the DOM."""
        try:
            tiles = self.driver.find_elements(By.CSS_SELECTOR, "div.order-summary-tile")
            return len(tiles)
        except:
            return 0
    
    def _check_oldest_date_on_page(self) -> Optional[datetime]:
        """Check the current page for order dates and return the oldest visible date."""
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            date_elements = soup.find_all('span', class_='placed-order-summary-ts')
            
            if not date_elements:
                return None
            
            dates = []
            for element in date_elements:
                date_text = element.get_text(strip=True)
                parsed_date = self._parse_order_date_from_text(date_text)
                if parsed_date:
                    dates.append(parsed_date)
            
            return min(dates) if dates else None
        except Exception:
            return None
    
    def _scroll_to_load_all_content(self, max_scrolls: int = 100) -> None:
        """Scroll to load all order history content."""
        self._log(f"Scrolling to load content (cutoff: {self.cutoff_date.strftime('%Y-%m-%d')})...")
        self.hit_date_cutoff = False
        
        try:
            scrollable_element = self._get_scrollable_element()
            
            if not scrollable_element:
                self._log("Could not find scrollable container", "ERROR")
                return
            
            scroll_count = 0
            last_tile_count = self._count_order_tiles()
            
            self._log(f"Initial tile count: {last_tile_count}")
            
            while scroll_count < max_scrolls:
                try:
                    self.driver.execute_script(
                        "arguments[0].scrollTop = arguments[0].scrollHeight", 
                        scrollable_element
                    )
                except:
                    scrollable_element = self._get_scrollable_element()
                    if not scrollable_element:
                        break
                    continue
                
                time.sleep(0.1)
                scroll_count += 1
                new_tile_count = self._count_order_tiles()
                
                if new_tile_count == last_tile_count:
                    time.sleep(0.3)
                    new_tile_count = self._count_order_tiles()
                
                if scroll_count % 5 == 0:
                    oldest_date = self._check_oldest_date_on_page()
                    if oldest_date and oldest_date < self.cutoff_date:
                        self._log(f"Scroll {scroll_count}: Hit date cutoff (oldest: {oldest_date.strftime('%Y-%m-%d')})")
                        self.hit_date_cutoff = True
                        break
                
                if new_tile_count == last_tile_count:
                    try:
                        self.driver.execute_script("arguments[0].scrollTop = 0", scrollable_element)
                        time.sleep(0.3)
                        self.driver.execute_script(
                            "arguments[0].scrollTop = arguments[0].scrollHeight", 
                            scrollable_element
                        )
                        time.sleep(0.3)
                    except:
                        break
                    
                    new_tile_count = self._count_order_tiles()
                    
                    if new_tile_count == last_tile_count:
                        oldest_date = self._check_oldest_date_on_page()
                        if oldest_date and oldest_date < self.cutoff_date:
                            self.hit_date_cutoff = True
                        break
                
                last_tile_count = new_tile_count
            
            final_tile_count = self._count_order_tiles()
            try:
                final_height = self.driver.execute_script("return arguments[0].scrollHeight", scrollable_element)
            except:
                final_height = 0
            
            self.run_stats["scroll_count"] = scroll_count
            self.run_stats["final_scroll_height"] = final_height
            self.run_stats["final_tile_count"] = final_tile_count
            
        except Exception as e:
            self._log(f"Error during scrolling: {e}", "ERROR")
    
    def _extract_part_numbers_from_order_history(self) -> List[str]:
        """Extract part numbers from the loaded order history."""
        self._log("Extracting part numbers from DOM...")
        url_paths = []
        
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            order_tiles = soup.find_all('div', class_='order-summary-tile')
            
            self.run_stats["total_order_tiles"] = len(order_tiles)
            self._log(f"Found {len(order_tiles)} order tiles")
            
            for order_tile in order_tiles:
                self.run_stats["orders_processed"] += 1
                
                date_element = order_tile.find('span', class_='placed-order-summary-ts')
                
                if not date_element:
                    self.run_stats["orders_no_date"] += 1
                    continue
                
                date_text = date_element.get_text(strip=True)
                order_date = self._parse_order_date_from_text(date_text)
                
                if not order_date:
                    self.run_stats["orders_date_parse_fail"] += 1
                    continue
                
                if order_date < self.cutoff_date:
                    self.run_stats["orders_outside_range"] += 1
                    continue
                
                self.run_stats["orders_in_range"] += 1
                part_elements = order_tile.find_all(attrs={'data-mcm-partnumber': True})
                
                for element in part_elements:
                    base_part = element.get('data-mcm-partnumber', '').strip().upper()
                    comp_part_raw = element.get('data-mcm-comppartnumber', '')
                    comp_part = comp_part_raw.strip().upper() if comp_part_raw else None
                    
                    if base_part:
                        if comp_part:
                            url_path = f"{base_part}-{comp_part}"
                            display_part = comp_part
                            is_component = True
                            self.run_stats["component_parts_found"] = self.run_stats.get("component_parts_found", 0) + 1
                        else:
                            url_path = base_part
                            display_part = base_part
                            is_component = False
                        
                        url_paths.append(url_path)
                        
                        if url_path not in self.part_info_map:
                            self.part_info_map[url_path] = {
                                'display_part': display_part,
                                'base_part': base_part,
                                'url_path': url_path,
                                'is_component': is_component
                            }
            
            self.run_stats["total_parts_extracted"] = len(url_paths)
            
        except Exception as e:
            self._log(f"Error extracting parts: {e}", "ERROR")
        
        return url_paths
    
    def _count_and_filter_parts(self, url_paths: List[str]) -> Tuple[Dict[str, int], Dict[str, int]]:
        """Count occurrences and filter for min_order_count+ occurrences.
        
        Returns:
            Tuple of (filtered_parts, all_parts) where:
            - filtered_parts: dict of parts with min_order_count+ occurrences
            - all_parts: dict of ALL parts with their counts (unfiltered)
        """
        all_part_counts = dict(Counter(url_paths))
        frequent_parts = {url_path: count for url_path, count in all_part_counts.items() if count >= self.min_order_count}
        self._log(f"Found {len(frequent_parts)} parts ordered {self.min_order_count}+ times (out of {len(all_part_counts)} unique parts)")
        return frequent_parts, all_part_counts
    
    def _click_connect_to_sales_workstation(self) -> bool:
        """Click the 'Connect to Sales Workstation' button."""
        try:
            connect_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Connect to Sales Workstation')]"))
            )
            connect_button.click()
            return True
        except Exception as e:
            self._log(f"Error clicking connect button: {e}", "ERROR")
            return False
    
    def _click_order_history_tab(self) -> bool:
        """Click the 'Order History' tab in the masthead navigation."""
        try:
            self.driver.switch_to.default_content()
            WebDriverWait(self.driver, 30).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            order_history_link = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.ID, "ShellLayout_OrdHist_Btn"))
            )
            
            self.driver.execute_script("arguments[0].scrollIntoView(true);", order_history_link)
            time.sleep(0.2)
            
            try:
                order_history_link.click()
            except:
                self.driver.execute_script("arguments[0].click();", order_history_link)
            
            time.sleep(1)
            return True
            
        except Exception as e:
            self._log(f"Error clicking Order History tab: {e}", "ERROR")
            return False
    
    def _setup_sales_workstation_and_order_history(self) -> bool:
        """One-time setup: Open Sales Workstation and Order History."""
        try:
            self._log("Opening Sales Workstation...")
            self.driver.get("https://salesworkstation/")
            time.sleep(1)
            
            if not self._verify_sales_workstation_loaded():
                self._log("Failed to load Sales Workstation", "ERROR")
                return False
            
            self._log("Opening Order History tab...")
            self.driver.execute_script("window.open('https://www.mcmaster.com/order-history/', '_blank');")
            time.sleep(0.5)
            
            order_history_tab = self.driver.window_handles[-1]
            self.driver.switch_to.window(order_history_tab)
            time.sleep(1)
            
            self._log("Attempting to connect to Sales Workstation...")
            if self._click_connect_to_sales_workstation():
                self._log("Successfully clicked connect button")
            else:
                self._log("Connect button not found or already connected - continuing anyway", "WARN")
            
            time.sleep(1)
            return True
            
        except Exception as e:
            self._log(f"Error during Sales Workstation setup: {e}", "ERROR")
            return False
    
    def _analyze_single_account_orders(self, account_number: str) -> List[str]:
        """Analyze a single customer account's order history."""
        self._log(f"Processing account: {account_number}")
        
        self.run_stats = {"page_reload_attempts": 0}
        
        try:
            self._navigate_mainframe_icma(account_number)
            
            order_history_tab = self.driver.window_handles[-1]
            self.driver.switch_to.window(order_history_tab)
            time.sleep(0.5)
            
            url_paths = []
            max_page_attempts = MAX_ORDER_HISTORY_ATTEMPTS
            
            for attempt in range(1, max_page_attempts + 1):
                self._log(f"Page attempt {attempt}/{max_page_attempts}")
                
                page_reloads = self.run_stats.get("page_reload_attempts", 0)
                self._reset_stats()
                self.run_stats["page_reload_attempts"] = page_reloads
                
                if attempt > 1:
                    self._log("Refreshing page for clean state...")
                    self.driver.refresh()
                    time.sleep(1)
                
                if not self._click_order_history_tab():
                    self._log(f"Failed to click Order History tab", "ERROR")
                    if attempt < max_page_attempts:
                        self.run_stats["page_reload_attempts"] += 1
                        time.sleep(1)
                        continue
                    break
                
                time.sleep(1)
                
                initial_tile_count = self._count_order_tiles()
                
                if initial_tile_count == 0:
                    self._log(f"Order history empty, retrying...", "WARN")
                    self.run_stats["page_reload_attempts"] += 1
                    if attempt < max_page_attempts:
                        time.sleep(1)
                        continue
                    break
                
                WebDriverWait(self.driver, 20).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                
                self._scroll_to_load_all_content()
                url_paths = self._extract_part_numbers_from_order_history()
                
                if self.hit_date_cutoff:
                    self._log(f"Successfully hit date cutoff - data is complete")
                    break
                else:
                    self._log(f"Did NOT hit date cutoff - infinite scroll may have failed", "WARN")
                    self.run_stats["page_reload_attempts"] += 1
                    if attempt < max_page_attempts:
                        self._log(f"Retrying page load...")
                        time.sleep(1)
                        continue
                    else:
                        break
            
            if url_paths:
                self._log(f"Account {account_number}: {len(url_paths)} parts extracted")
            else:
                self._log(f"Account {account_number}: No parts extracted", "WARN")
            
            return url_paths
            
        except Exception as e:
            self._log(f"Error during order analysis for account {account_number}: {e}", "ERROR")
            return []
    
    def _analyze_customer_orders(self, account_numbers: List[str]) -> Tuple[List[Tuple[str, int]], Dict[str, int], List[str], List[str]]:
        """Analyze order history for multiple customer accounts.
        
        Returns:
            Tuple of (frequent_parts_list, all_part_counts, successful_accounts, failed_accounts)
        """
        self._log("="*80)
        self._log("PHASE 1: ANALYZING CUSTOMER ORDER HISTORY")
        self._log(f"Accounts to process: {', '.join(account_numbers)}")
        self._log("="*80)
        
        if not self._setup_sales_workstation_and_order_history():
            self._log("Failed to set up Sales Workstation - cannot continue", "ERROR")
            return [], {}, [], account_numbers
        
        all_url_paths = []
        successful_accounts = []
        failed_accounts = []
        previous_parts_set = None
        
        for account_number in account_numbers:
            self._log(f"\n--- Processing account {account_number} ---")
            try:
                url_paths = self._analyze_single_account_orders(account_number)
                
                if url_paths:
                    current_parts_set = set(url_paths)
                    if previous_parts_set is not None and current_parts_set == previous_parts_set:
                        self._log(f"WARNING: Account {account_number} returned identical parts to previous account!", "WARN")
                    
                    previous_parts_set = current_parts_set
                    all_url_paths.extend(url_paths)
                    successful_accounts.append(account_number)
                else:
                    failed_accounts.append(account_number)
                    
            except Exception as e:
                failed_accounts.append(account_number)
                self._log(f"Account {account_number}: Failed with error: {e}", "ERROR")
        
        if not all_url_paths:
            self._log("No parts extracted from any account", "ERROR")
            return [], {}, successful_accounts, failed_accounts
        
        frequent_parts, all_part_counts = self._count_and_filter_parts(all_url_paths)
        sorted_parts = sorted(frequent_parts.items(), key=lambda x: x[1], reverse=True)
        
        # Store all part counts for distribution report
        self.all_part_counts = all_part_counts
        
        return sorted_parts, all_part_counts, successful_accounts, failed_accounts
    
    # ==================== PART SCRAPING METHODS (SELENIUM - SEQUENTIAL) ====================
    
    def _scrape_mcmaster_part(self, url_path: str) -> Dict[str, Any]:
        """Scrape McMaster website for part information using Selenium."""
        url = f"https://www.mcmaster.com/{url_path}/"
        try:
            self.driver.get(url)
            wait = WebDriverWait(self.driver, 5)
            
            current_url = self.driver.current_url.lower()
            if '/search/' in current_url or 'nav/action' in current_url:
                return {
                    'Part Number': url_path,
                    'Parent Description': 'Not Found',
                    'Suffix Description': 'Not Found',
                    'Image URL': 'Not Found',
                    'Status': 'Invalid part number (redirected to search)'
                }
            
            page_source_lower = self.driver.page_source.lower()
            if 'no results' in page_source_lower or 'did not match any products' in page_source_lower:
                return {
                    'Part Number': url_path,
                    'Parent Description': 'Not Found',
                    'Suffix Description': 'Not Found',
                    'Image URL': 'Not Found',
                    'Status': 'Invalid part number (no results)'
                }
            
            # Extract parent description
            try:
                parent_elem = wait.until(EC.presence_of_element_located((
                    By.CSS_SELECTOR,
                    'h1._productDetailHeaderPrimary_1ijr6_17, h1[class*="productDetailHeaderPrimary"]'
                )))
                parent_description = parent_elem.text.strip()
            except:
                parent_description = "Not Found"
            
            # Extract suffix description
            try:
                suffix_elem = self.driver.find_element(
                    By.CSS_SELECTOR,
                    'h3._productDetailHeaderSecondary_1ijr6_31, h3[class*="productDetailHeaderSecondary"]'
                )
                suffix_description = suffix_elem.text.strip()
            except:
                suffix_description = "Not Found"
            
            # Extract image URL
            image_url = "Not Found"
            try:
                selectors_to_try = [
                    'img[class*="printProductImage"]',
                    'div[class*="ImageContainer"] img',
                    'div[class*="imageContainer"] img',
                    'img._img_j8npf_109',
                    'img[class*="_img_"]'
                ]
                img_elem = None
                for selector in selectors_to_try:
                    try:
                        img_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if img_elem:
                            break
                    except:
                        continue
                
                if not img_elem:
                    raise Exception("No image element found")
                
                image_url = img_elem.get_attribute('src')
                
                # Try to get higher resolution from srcset
                try:
                    srcset = img_elem.get_attribute('srcset')
                    if srcset:
                        urls = []
                        for item in srcset.split(','):
                            parts = item.strip().split(' ')
                            if parts:
                                urls.append(parts[0])
                        if urls:
                            high_res_url = urls[-1]
                            if high_res_url:
                                image_url = high_res_url
                except:
                    pass
                
                if image_url and not image_url.startswith('http'):
                    image_url = f"https://www.mcmaster.com{image_url}"
            except Exception as e:
                image_url = "Not Found"
            
            if parent_description == "Not Found" and suffix_description == "Not Found":
                status = "Part not found or page structure changed"
            else:
                status = "Success"
            
            return {
                'Part Number': url_path,
                'Parent Description': parent_description,
                'Suffix Description': suffix_description,
                'Image URL': image_url,
                'Status': status
            }
            
        except Exception as e:
            self._log(f"Error scraping part {url_path}: {e}", "ERROR")
            return {
                'Part Number': url_path,
                'Parent Description': 'Error',
                'Suffix Description': 'Error',
                'Image URL': 'Error',
                'Status': f'Failed: {str(e)}'
            }
    
    def _scrape_mcmaster_part_with_retry(self, url_path: str) -> Dict[str, Any]:
        """Scrape McMaster website with one retry on failure."""
        scrape_data = self._scrape_mcmaster_part(url_path)
        
        has_failure = (
            scrape_data['Parent Description'] in ['Not Found', 'Error'] or
            scrape_data['Image URL'] in ['Not Found', 'Error'] or
            scrape_data['Status'] != 'Success'
        )
        
        if has_failure:
            self._log(f"First scrape attempt had failures for {url_path}, retrying...", "WARN")
            time.sleep(0.5)
            scrape_data = self._scrape_mcmaster_part(url_path)
            
            if scrape_data['Status'] != 'Success':
                self._log(f"Retry also failed for {url_path}", "WARN")
        
        return scrape_data
    
    def _download_image(self, image_url: str, part_identifier: str) -> Optional[Image.Image]:
        """Download product image from URL."""
        if image_url == "Not Found" or image_url == "Error":
            return None
        try:
            response = requests.get(image_url, timeout=10, verify=False)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content))
            return img
        except Exception as e:
            self._log(f"Error downloading image for {part_identifier}: {e}", "WARN")
            return None
    
    # ==================== LABEL GENERATION METHODS (THREADED) ====================
    
    def _generate_qr_code(self, link: str) -> Image.Image:
        """Generate QR code using thread-local session."""
        try:
            session = self._get_thread_session()
            api_url = f"https://api.qrserver.com/v1/create-qr-code/?size=400x400&data={link}"
            response = session.get(api_url, timeout=10, verify=False)
            response.raise_for_status()
            qr_img = Image.open(BytesIO(response.content))
            return qr_img
        except Exception as e:
            return Image.new('RGB', (400, 400), 'white')
    
    def _wrap_text(self, text: str, font, draw, max_width: int) -> List[str]:
        """Wrap text to fit within max_width."""
        words = text.split()
        lines = []
        current_line = []
        for word in words:
            test_line = ' '.join(current_line + [word])
            if draw.textlength(test_line, font=font) <= max_width:
                current_line.append(word)
            else:
                if current_line:
                    lines.append(' '.join(current_line))
                current_line = [word]
        if current_line:
            lines.append(' '.join(current_line))
        return lines if lines else [text]
    
    def _generate_label_small(self, display_part, url_path, item_header, additional_info, 
                               product_image, labels_folder, filename_part_number=None, customer_ref=None):
        """Generate small label (0.925 x 2.75 in), rotated 90 degrees."""
        file_part_number = filename_part_number or display_part
        has_customer_ref = customer_ref is not None and customer_ref.strip() != ''
        
        link = f"https://www.mcmaster.com/{url_path}/?mode=QR"
        qr_img = self._generate_qr_code(link)
        
        product_img = None
        if product_image:
            try:
                if product_image.mode == 'P':
                    product_img = product_image.convert("RGBA")
                    background = Image.new('RGB', product_img.size, (255, 255, 255))
                    background.paste(product_img, mask=product_img.split()[3] if len(product_img.split()) == 4 else None)
                    product_img = background
                else:
                    product_img = product_image.convert("RGB")
                product_img.thumbnail((250, 250), Image.Resampling.LANCZOS)
            except Exception:
                product_img = None
        
        qr_size = 145
        qr_img = qr_img.resize((qr_size, qr_size), Image.Resampling.LANCZOS)
        
        label_width = 825
        label_height = 278
        label = Image.new('RGB', (label_width, label_height), 'white')
        draw = ImageDraw.Draw(label)
        
        try:
            if has_customer_ref:
                header_font = ImageFont.truetype("arialbd.ttf", 26)
                info_font = ImageFont.truetype("arial.ttf", 22)
                part_font = ImageFont.truetype("arial.ttf", 22)
                ref_font = ImageFont.truetype("arial.ttf", 22)
            else:
                header_font = ImageFont.truetype("arialbd.ttf", 28)
                info_font = ImageFont.truetype("arial.ttf", 24)
                part_font = ImageFont.truetype("arial.ttf", 24)
                ref_font = None
        except:
            header_font = ImageFont.load_default()
            info_font = ImageFont.load_default()
            part_font = ImageFont.load_default()
            ref_font = ImageFont.load_default() if has_customer_ref else None
        
        if product_img:
            img_x = 50
            img_y = (label_height - product_img.height) // 2
            label.paste(product_img, (img_x, img_y))
        
        text_start_x = 340
        text_y = 11
        qr_x = label_width - qr_size - 6
        qr_right_edge = qr_x + qr_size
        max_header_width = qr_right_edge - text_start_x - 15
        max_text_width = qr_x - text_start_x - 15
        
        header_line_height = 30 if has_customer_ref else 32
        info_line_height = 24 if has_customer_ref else 26
        part_line_height = 24 if has_customer_ref else 26
        
        header_lines = self._wrap_text(item_header, header_font, draw, max_header_width)
        for line in header_lines:
            draw.text((text_start_x, text_y), line, fill='black', font=header_font)
            text_y += header_line_height
        
        text_y += 4 if has_customer_ref else 6
        
        if additional_info and additional_info not in ['Not Found', 'Error']:
            info_lines = self._wrap_text(additional_info, info_font, draw, max_text_width)
            for line in info_lines:
                draw.text((text_start_x, text_y), line, fill='black', font=info_font)
                text_y += info_line_height
        
        text_y += 6 if has_customer_ref else 10
        
        part_lines = self._wrap_text(display_part, part_font, draw, max_text_width)
        for line in part_lines:
            draw.text((text_start_x, text_y), line, fill='black', font=part_font)
            text_y += part_line_height
        
        if has_customer_ref and ref_font:
            text_y += 4
            ref_text = f"Your ref: {customer_ref}"
            ref_lines = self._wrap_text(ref_text, ref_font, draw, max_text_width)
            for line in ref_lines:
                draw.text((text_start_x, text_y), line, fill='black', font=ref_font)
                text_y += part_line_height
        
        logo_img = self._load_mcmaster_logo()
        if logo_img:
            try:
                logo_img_resized = logo_img.copy()
                logo_img_resized.thumbnail((150, 30), Image.Resampling.LANCZOS)
                logo_x = text_start_x
                logo_y = label_height - logo_img_resized.height - 22
                if logo_img_resized.mode == 'RGBA':
                    label.paste(logo_img_resized, (logo_x, logo_y), logo_img_resized)
                else:
                    label.paste(logo_img_resized, (logo_x, logo_y))
            except Exception:
                pass
        
        qr_y = label_height - qr_size - 22
        label.paste(qr_img, (qr_x, qr_y))
        
        label_rotated = label.rotate(90, expand=True)
        
        filename = os.path.join(labels_folder, f"{file_part_number}.png")
        label_rotated.save(filename, quality=95, optimize=False)
        
        return filename
    
    def _generate_label_medium(self, display_part, url_path, item_header, additional_info,
                                product_image, labels_folder, filename_part_number=None, customer_ref=None):
        """Generate medium label (3.75 x 1.5 in), horizontal layout."""
        file_part_number = filename_part_number or display_part
        has_customer_ref = customer_ref is not None and customer_ref.strip() != ''
        
        link = f"https://www.mcmaster.com/{url_path}/?mode=QR"
        qr_img = self._generate_qr_code(link)
        
        product_img = None
        if product_image:
            try:
                if product_image.mode == 'P':
                    product_img = product_image.convert("RGBA")
                    background = Image.new('RGB', product_img.size, (255, 255, 255))
                    background.paste(product_img, mask=product_img.split()[3] if len(product_img.split()) == 4 else None)
                    product_img = background
                else:
                    product_img = product_image.convert("RGB")
                product_img.thumbnail((340, 340), Image.Resampling.LANCZOS)
            except Exception:
                product_img = None
        
        qr_size = 200
        qr_img = qr_img.resize((qr_size, qr_size), Image.Resampling.LANCZOS)
        
        label_width = 1125
        label_height = 450
        label = Image.new('RGB', (label_width, label_height), 'white')
        draw = ImageDraw.Draw(label)
        
        try:
            if has_customer_ref:
                header_font = ImageFont.truetype("arialbd.ttf", 36)
                info_font = ImageFont.truetype("arial.ttf", 28)
                part_font = ImageFont.truetype("arial.ttf", 28)
                ref_font = ImageFont.truetype("arial.ttf", 28)
            else:
                header_font = ImageFont.truetype("arialbd.ttf", 40)
                info_font = ImageFont.truetype("arial.ttf", 32)
                part_font = ImageFont.truetype("arial.ttf", 32)
                ref_font = None
        except:
            header_font = ImageFont.load_default()
            info_font = ImageFont.load_default()
            part_font = ImageFont.load_default()
            ref_font = ImageFont.load_default() if has_customer_ref else None
        
        if product_img:
            img_x = 70
            img_y = (label_height - product_img.height) // 2
            label.paste(product_img, (img_x, img_y))
        
        text_start_x = 465
        text_y = 30
        qr_x = label_width - qr_size - 35
        qr_right_edge = qr_x + qr_size
        max_header_width = qr_right_edge - text_start_x - 20
        max_text_width = qr_x - text_start_x - 20
        
        header_line_height = 46 if has_customer_ref else 52
        info_line_height = 36 if has_customer_ref else 42
        part_line_height = 36 if has_customer_ref else 42
        
        header_lines = self._wrap_text(item_header, header_font, draw, max_header_width)
        for line in header_lines:
            draw.text((text_start_x, text_y), line, fill='black', font=header_font)
            text_y += header_line_height
        
        text_y += 4 if has_customer_ref else 6
        
        if additional_info and additional_info not in ['Not Found', 'Error']:
            info_lines = self._wrap_text(additional_info, info_font, draw, max_text_width)
            for line in info_lines:
                draw.text((text_start_x, text_y), line, fill='black', font=info_font)
                text_y += info_line_height
        
        text_y += 6 if has_customer_ref else 10
        
        part_lines = self._wrap_text(display_part, part_font, draw, max_text_width)
        for line in part_lines:
            draw.text((text_start_x, text_y), line, fill='black', font=part_font)
            text_y += part_line_height
        
        if has_customer_ref and ref_font:
            text_y += 6
            ref_text = f"Your ref: {customer_ref}"
            ref_lines = self._wrap_text(ref_text, ref_font, draw, max_text_width)
            for line in ref_lines:
                draw.text((text_start_x, text_y), line, fill='black', font=ref_font)
                text_y += part_line_height
        
        logo_img = self._load_mcmaster_logo()
        if logo_img:
            try:
                logo_img_resized = logo_img.copy()
                logo_img_resized.thumbnail((300, 60), Image.Resampling.LANCZOS)
                logo_x = text_start_x
                logo_y = label_height - logo_img_resized.height - 30
                if logo_img_resized.mode == 'RGBA':
                    label.paste(logo_img_resized, (logo_x, logo_y), logo_img_resized)
                else:
                    label.paste(logo_img_resized, (logo_x, logo_y))
            except Exception:
                pass
        
        qr_y = label_height - qr_size - 30
        label.paste(qr_img, (qr_x, qr_y))
        
        filename = os.path.join(labels_folder, f"{file_part_number}.png")
        label.save(filename, quality=95, optimize=False)
        
        return filename
    
    def _generate_label_for_size(self, size: str, display_part, url_path, item_header, additional_info,
                                  product_image, labels_folder, filename_part_number=None, customer_ref=None):
        """Generate label for a specific size."""
        if size == "medium":
            return self._generate_label_medium(display_part, url_path, item_header, additional_info,
                                               product_image, labels_folder, filename_part_number, customer_ref)
        else:
            return self._generate_label_small(display_part, url_path, item_header, additional_info,
                                              product_image, labels_folder, filename_part_number, customer_ref)
    
    def _create_single_label_task(self, size: str, url_path: str, cached_data: Dict, 
                                   labels_folder: str) -> Dict[str, Any]:
        """Worker function for threaded label generation."""
        scrape_data = cached_data['scrape_data']
        product_image = cached_data['product_image']
        count = cached_data['count']
        customer_ref = cached_data['customer_ref']
        part_info = cached_data['part_info']
        display_part = part_info['display_part']
        
        scrape_fully_successful = (
            scrape_data['Status'] == 'Success' and
            scrape_data['Parent Description'] not in ['Not Found', 'Error'] and
            scrape_data['Image URL'] not in ['Not Found', 'Error'] and
            product_image is not None
        )
        
        result = {
            "display_part": display_part,
            "url_path": url_path,
            "order_count": count,
            "label_file": None,
            "status": "fail",
            "include_in_pdf": False,
            "customer_ref": customer_ref,
            "is_component": part_info['is_component'],
            "failure_reason": None,
            "failure_details": None
        }

        try:
            label_filename = self._generate_label_for_size(
                size=size,
                display_part=display_part,
                url_path=url_path,
                item_header=scrape_data['Parent Description'],
                additional_info=scrape_data['Suffix Description'],
                product_image=product_image,
                labels_folder=labels_folder,
                filename_part_number=display_part,
                customer_ref=customer_ref
            )
            
            include_in_pdf = scrape_fully_successful
            
            result["label_file"] = label_filename
            result["status"] = "success" if include_in_pdf else "partial (excluded from PDF)"
            result["include_in_pdf"] = include_in_pdf
            
            if not include_in_pdf:
                result["failure_reason"] = "Scraping incomplete"
                result["failure_details"] = {
                    "parent_description": scrape_data['Parent Description'],
                    "image_url": scrape_data['Image URL'],
                    "image_downloaded": product_image is not None
                }
                    
        except Exception as e:
            result["status"] = f"failed: {str(e)}"
            result["failure_reason"] = f"Label generation error: {str(e)}"
            
        return result
    
    # ==================== CSV/EXCEL EXPORT METHODS ====================
    
    def _generate_order_distribution_excel(self, all_part_counts: Dict[str, int], 
                                            output_folder: Path, account_numbers: List[str]) -> str:
        """Generate Excel file with order distribution statistics and part list.
        
        Sheet 1: Distribution summary (unique parts count, parts ordered N+ times)
        Sheet 2: Alphabetical list of all parts with order counts
        """
        excel_filename = "order_distribution.xlsx"
        excel_path = output_folder / excel_filename
        
        self._log(f"Generating order distribution Excel: {excel_path}")
        
        total_unique = len(all_part_counts)
        
        # Calculate distribution stats (2+ through 10+)
        distribution_data = []
        distribution_data.append({
            'Metric': 'Total Unique Parts Ordered',
            'Count': total_unique
        })
        
        for threshold in range(2, 11):
            count_at_threshold = sum(1 for c in all_part_counts.values() if c >= threshold)
            distribution_data.append({
                'Metric': f'Parts Ordered {threshold}+ Times',
                'Count': count_at_threshold
            })
        
        df_distribution = pd.DataFrame(distribution_data)
        
        # Build alphabetical part list with display_part for sorting
        part_list_data = []
        for url_path, count in all_part_counts.items():
            part_info = self.part_info_map.get(url_path, {
                'display_part': url_path,
                'base_part': url_path,
                'url_path': url_path,
                'is_component': False
            })
            part_list_data.append({
                'Part Number': part_info['display_part'],
                'Order Count': count
            })
        
        # Sort alphabetically by part number
        part_list_data.sort(key=lambda x: x['Part Number'])
        df_part_list = pd.DataFrame(part_list_data)
        
        # Write to Excel with two sheets
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df_distribution.to_excel(writer, sheet_name='Distribution Summary', index=False)
            df_part_list.to_excel(writer, sheet_name='Part List', index=False)
        
        self._log(f"Order distribution Excel generated with {total_unique} unique parts")
        return str(excel_path)
    
    def _generate_parts_csv(self, frequent_parts: List[Tuple[str, int]], scraped_data_cache: Dict,
                            output_folder: Path, account_numbers: List[str]) -> str:
        """Generate CSV file with all parts and their information."""
        csv_filename = "scrape_info_sheet.csv"
        csv_path = output_folder / csv_filename
        
        self._log(f"Generating parts CSV: {csv_path}")
        
        rows = []
        for url_path, count in sorted(frequent_parts, key=lambda x: x[0]):
            cached = scraped_data_cache.get(url_path, {})
            scrape_data = cached.get('scrape_data', {})
            
            part_info = self.part_info_map.get(url_path, {
                'display_part': url_path,
                'base_part': url_path,
                'url_path': url_path,
                'is_component': False
            })
            
            row = {
                'Part Number': part_info['display_part'],
                'Base Part Number': part_info['base_part'],
                'Order Count': count,
                'Parent Description': scrape_data.get('Parent Description', 'N/A'),
                'Suffix Description': scrape_data.get('Suffix Description', 'N/A'),
                'Scrape Status': scrape_data.get('Status', 'N/A'),
                'Included in PDF': 'Yes' if (
                    scrape_data.get('Status') == 'Success' and
                    scrape_data.get('Parent Description') not in ['Not Found', 'Error'] and
                    scrape_data.get('Image URL') not in ['Not Found', 'Error'] and
                    cached.get('product_image') is not None
                ) else 'No',
                'Customer Reference': cached.get('customer_ref', '')
            }
            rows.append(row)
        
        df = pd.DataFrame(rows)
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        self._log(f"CSV generated with {len(rows)} parts")
        return str(csv_path)
    
    # ==================== PDF GENERATION ====================
    
    def _generate_pdf(self, label_files: List[str], output_folder: Path, size: str):
        """Generate PDF with all labels on sticker sheets."""
        config = LABEL_CONFIGS[size]
        dimension_str = config["pdf_dimension_str"]
        pdf_path = output_folder / f"labels_{dimension_str}.pdf"
        
        dpi = config["dpi"]
        
        page_width_inches = 8.5
        page_height_inches = 11
        page_width_px = int(page_width_inches * dpi)
        page_height_px = int(page_height_inches * dpi)
        
        label_width_px = int(config["label_width_inches"] * dpi)
        label_height_px = int(config["label_height_inches"] * dpi)
        left_margin_px = int(config["left_margin_inches"] * dpi)
        top_margin_px = int(config["top_margin_inches"] * dpi)
        
        if "gap_inches" in config:
            horizontal_gap_px = int(config["gap_inches"] * dpi)
            vertical_gap_px = int(config["gap_inches"] * dpi)
        else:
            horizontal_gap_px = int(config["horizontal_gap_inches"] * dpi)
            vertical_gap_px = int(config["vertical_gap_inches"] * dpi)
        
        labels_per_row = config["labels_per_row"]
        labels_per_column = config["labels_per_column"]
        labels_per_page = labels_per_row * labels_per_column
        
        self._log(f"PDF Layout ({size}): {labels_per_row}x{labels_per_column} = {labels_per_page} labels/page")
        
        pdf_images = []
        current_page = Image.new('RGB', (page_width_px, page_height_px), 'white')
        label_index = 0
        
        for label_file in label_files:
            if not os.path.exists(label_file):
                continue
            
            page_position = label_index % labels_per_page
            
            if label_index > 0 and page_position == 0:
                pdf_images.append(current_page)
                current_page = Image.new('RGB', (page_width_px, page_height_px), 'white')
            
            row = page_position // labels_per_row
            col = page_position % labels_per_row
            
            x = left_margin_px + col * (label_width_px + horizontal_gap_px)
            y = top_margin_px + row * (label_height_px + vertical_gap_px)
            
            label_img = Image.open(label_file)
            label_img_resized = label_img.resize((label_width_px, label_height_px), Image.Resampling.LANCZOS)
            current_page.paste(label_img_resized, (x, y))
            
            label_index += 1
        
        if label_index > 0:
            pdf_images.append(current_page)
        
        if pdf_images:
            pdf_images[0].save(
                str(pdf_path),
                "PDF",
                resolution=dpi,
                save_all=True,
                append_images=pdf_images[1:] if len(pdf_images) > 1 else []
            )
            self._log(f"Generated PDF: {pdf_path}")
            return str(pdf_path)
        else:
            self._log("No labels to add to PDF", "WARN")
            return None
    
    # ==================== MAIN LABEL GENERATION (HYBRID APPROACH) ====================
    
    def _generate_labels_for_parts(self, frequent_parts: List[Tuple[str, int]], output_folder: Path,
                                    sizes_to_generate: List[str]):
        """
        Generate labels for all parts using hybrid approach:
        - Phase 1: Sequential Selenium scraping (reliable, avoids WAF)
        - Phase 2: Threaded label generation (fast QR codes + image processing)
        """
        self._log("="*80)
        self._log("PHASE 2: GENERATING LABELS (Hybrid: Sequential Scrape + Threaded Labels)")
        self._log(f"Sizes to generate: {', '.join(sizes_to_generate)}")
        if self.customer_refs:
            self._log(f"Customer references available for {len(self.customer_refs)} parts")
        self._log("="*80)
        
        # Sort by display_part for alphabetical ordering
        def get_sort_key(item):
            url_path = item[0]
            part_info = self.part_info_map.get(url_path, {'display_part': url_path})
            return part_info['display_part']
        
        sorted_parts_alphabetical = sorted(frequent_parts, key=get_sort_key)
        self._log(f"Sorted {len(sorted_parts_alphabetical)} parts alphabetically by display part")
        
        # ========== PHASE 1: Sequential Selenium Scraping ==========
        self._log("\n--- Phase 1: Sequential Selenium Scraping ---")
        self._log("Creating headless browser for part scraping...")
        self.driver = self._create_webdriver(headless=True)
        
        scrape_results = []
        scraped_data_cache = {}
        scrape_success = 0
        scrape_fail = 0
        
        try:
            for i, (url_path, count) in enumerate(sorted_parts_alphabetical, 1):
                part_info = self.part_info_map.get(url_path, {
                    'display_part': url_path,
                    'base_part': url_path,
                    'url_path': url_path,
                    'is_component': False
                })
                display_part = part_info['display_part']
                
                self._log(f"Scraping {i}/{len(frequent_parts)}: {url_path} (display: {display_part})")
                
                scrape_data = self._scrape_mcmaster_part_with_retry(url_path)
                
                product_image = None
                if scrape_data['Image URL'] not in ['Not Found', 'Error']:
                    product_image = self._download_image(scrape_data['Image URL'], url_path)
                
                scrape_results.append(scrape_data)
                
                customer_ref = self.customer_refs.get(display_part) or self.customer_refs.get(url_path)
                
                scraped_data_cache[url_path] = {
                    'scrape_data': scrape_data,
                    'product_image': product_image,
                    'count': count,
                    'customer_ref': customer_ref,
                    'part_info': part_info
                }
                
                if scrape_data['Status'] == 'Success':
                    scrape_success += 1
                else:
                    scrape_fail += 1
                
                time.sleep(0.3)
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None
        
        self._log(f"Scraping complete: {scrape_success} success, {scrape_fail} failed")
        
        # ========== PHASE 2: Threaded Label Generation ==========
        self._log(f"\n--- Phase 2: Threaded Label Generation ({MAX_THREADS_LABELS} threads) ---")
        
        # Pre-load logo once (thread-safe since it's cached after first load)
        self._load_mcmaster_logo()
        
        all_results = {}
        self.failed_parts = []
        
        for size in sizes_to_generate:
            self._log(f"\n--- Generating {size.upper()} labels ---")
            
            labels_folder = output_folder / f"Labels_{size}"
            labels_folder.mkdir(exist_ok=True)
            
            label_results = []
            successful_label_files = []
            label_success = 0
            label_fail = 0
            
            # Use ThreadPoolExecutor for label generation
            with ThreadPoolExecutor(max_workers=MAX_THREADS_LABELS) as executor:
                futures = {
                    executor.submit(
                        self._create_single_label_task, 
                        size, 
                        url_path, 
                        cached, 
                        str(labels_folder)
                    ): url_path 
                    for url_path, cached in scraped_data_cache.items()
                }
                
                for i, future in enumerate(as_completed(futures), 1):
                    url_path = futures[future]
                    try:
                        result = future.result()
                        label_results.append(result)
                        
                        if result["include_in_pdf"]:
                            successful_label_files.append(result["label_file"])
                            label_success += 1
                        else:
                            label_fail += 1
                            # Only track failures once (on first size)
                            if size == sizes_to_generate[0] and result.get("failure_reason"):
                                self.failed_parts.append({
                                    "display_part": result["display_part"],
                                    "url_path": result["url_path"],
                                    "reason": result["failure_reason"],
                                    "details": result["failure_details"]
                                })
                        
                        if i % 20 == 0 or i == len(futures):
                            self._log(f"Generated {i}/{len(futures)} labels...")
                            
                    except Exception as exc:
                        self._log(f"Label generation failed for {url_path}: {exc}", "ERROR")
                        label_fail += 1
            
            # Sort label files alphabetically for consistent PDF ordering
            successful_label_files.sort()
            
            pdf_path = None
            if successful_label_files:
                try:
                    pdf_path = self._generate_pdf(successful_label_files, output_folder, size)
                except Exception as e:
                    self._log(f"Error generating PDF for {size}: {e}", "ERROR")
            
            all_results[size] = {
                "labels_folder": str(labels_folder),
                "pdf_path": pdf_path,
                "labels_in_pdf": label_success,
                "labels_excluded": label_fail,
                "label_results": label_results
            }
            
            self._log(f"{size.upper()}: {label_success} in PDF, {label_fail} excluded")
        
        return {
            "scrape_results": scrape_results,
            "scrape_successful": scrape_success,
            "scrape_failed": scrape_fail,
            "size_results": all_results,
            "failed_parts": self.failed_parts,
            "scraped_data_cache": scraped_data_cache
        }
    
    # ==================== MAIN RUN METHOD ====================
    
    def run(self, params):
        """Execute the combined tool's main logic."""
        account_number = params.get("account_number")
        excel_file_path = params.get("excel_file_path")
        
        if account_number and excel_file_path:
            raise ValueError("Provide either 'account_number' OR 'excel_file_path', not both.")
        if not account_number and not excel_file_path:
            raise ValueError("Must provide either 'account_number' or 'excel_file_path'.")
        
        use_excel_mode = excel_file_path is not None
        
        output_folder_str = params.get("output_folder")
        if not output_folder_str:
            raise ValueError("The 'output_folder' parameter cannot be empty.")
        
        output_folder = Path(output_folder_str)
        try:
            output_folder.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._log(f"Warning: Could not create output folder: {e}", "WARN")
            output_folder = Path(".")
        
        label_size = params.get("label_size", "small").lower()
        
        if label_size == "all":
            sizes_to_generate = ["small", "medium"]
        elif label_size in LABEL_CONFIGS:
            sizes_to_generate = [label_size]
        else:
            self._log(f"Invalid label_size '{label_size}', defaulting to 'small'", "WARN")
            sizes_to_generate = ["small"]
        
        self.label_size = label_size
        self._log(f"Label size(s) to generate: {', '.join(sizes_to_generate)}")
        
        self.failed_parts = []
        self.customer_refs = {}
        self.part_info_map = {}
        self.all_part_counts = {}
        
        try:
            if use_excel_mode:
                self._log("="*80)
                self._log("MODE: EXCEL FILE INPUT")
                self._log("="*80)
                
                part_numbers = self._read_part_numbers_from_excel(excel_file_path)
                frequent_parts = [(part, 1) for part in part_numbers]
                all_part_counts = {part: 1 for part in part_numbers}
                successful_accounts = []
                failed_accounts = []
                
            else:
                self._log("="*80)
                self._log("MODE: ACCOUNT ORDER HISTORY")
                self._log("="*80)
                
                account_numbers = [acc.strip() for acc in account_number.split(',') if acc.strip()]
                
                if not account_numbers:
                    raise ValueError("No valid account numbers provided")
                
                self._log(f"Parsed {len(account_numbers)} account number(s): {', '.join(account_numbers)}")
                
                lookback_days = params.get("lookback_days", 365)
                self.cutoff_date = datetime.now() - timedelta(days=lookback_days)
                
                min_order_count = params.get("min_order_count", 2)
                if min_order_count < 1:
                    min_order_count = 2
                self.min_order_count = min_order_count
                
                self.driver = self._create_webdriver(headless=False)
                
                try:
                    frequent_parts, all_part_counts, successful_accounts, failed_accounts = self._analyze_customer_orders(account_numbers)
                finally:
                    if self.driver:
                        self.driver.quit()
                        self.driver = None
                
                if not frequent_parts:
                    return {
                        "status": "warning",
                        "message": f"No parts ordered {self.min_order_count}+ times across all accounts",
                        "data": {
                            "account_numbers": account_numbers,
                            "successful_accounts": successful_accounts,
                            "failed_accounts": failed_accounts,
                            "lookback_days": lookback_days,
                            "min_order_count": self.min_order_count,
                            "frequent_parts_count": 0
                        }
                    }
            
            if not frequent_parts:
                return {
                    "status": "warning",
                    "message": "No parts to process",
                    "data": {"frequent_parts_count": 0}
                }
            
            # Generate order distribution Excel right after scraping (account mode only)
            order_distribution_path = None
            if not use_excel_mode and all_part_counts:
                try:
                    order_distribution_path = self._generate_order_distribution_excel(
                        all_part_counts,
                        output_folder,
                        account_numbers
                    )
                except Exception as e:
                    self._log(f"Error generating order distribution Excel: {e}", "ERROR")
            
            label_data = self._generate_labels_for_parts(frequent_parts, output_folder, sizes_to_generate)
            
            csv_path = None
            if not use_excel_mode:
                try:
                    csv_path = self._generate_parts_csv(
                        frequent_parts, 
                        label_data["scraped_data_cache"], 
                        output_folder,
                        account_numbers
                    )
                except Exception as e:
                    self._log(f"Error generating CSV: {e}", "ERROR")
            
            component_count = sum(1 for url_path in dict(frequent_parts).keys() 
                                  if self.part_info_map.get(url_path, {}).get('is_component', False))
            
            result_data = {
                "analysis_timestamp": datetime.now().isoformat(),
                "label_sizes": sizes_to_generate,
                "frequent_parts_count": len(frequent_parts),
                "component_parts_count": component_count,
                "frequent_parts": frequent_parts,
                "scrape_successful": label_data["scrape_successful"],
                "scrape_failed": label_data["scrape_failed"],
                "size_results": label_data["size_results"],
                "failed_parts": label_data["failed_parts"]
            }
            
            if use_excel_mode:
                result_data["excel_file_path"] = excel_file_path
                result_data["input_mode"] = "excel"
                result_data["customer_refs_count"] = len(self.customer_refs)
            else:
                result_data["account_numbers"] = account_numbers
                result_data["successful_accounts"] = successful_accounts
                result_data["failed_accounts"] = failed_accounts
                result_data["lookback_days"] = params.get("lookback_days", 365)
                result_data["cutoff_date"] = self.cutoff_date.strftime('%Y-%m-%d')
                result_data["min_order_count"] = self.min_order_count
                result_data["input_mode"] = "account"
                result_data["scrape_info_sheet_path"] = csv_path
                result_data["order_distribution_path"] = order_distribution_path
                result_data["total_unique_parts"] = len(all_part_counts)
            
            total_in_pdf = sum(r["labels_in_pdf"] for r in label_data["size_results"].values())
            total_excluded = sum(r["labels_excluded"] for r in label_data["size_results"].values())
            
            return {
                "status": "success",
                "message": f"Generated labels for {len(sizes_to_generate)} size(s): {total_in_pdf} in PDF per size, {total_excluded} excluded ({component_count} component parts)",
                "data": result_data
            }
            
        except Exception as e:
            self._log(f"Error during execution: {e}", "ERROR")
            raise
        finally:
            if self.driver:
                self.driver.quit()


def print_results(result: Dict):
    """Print analysis and label generation results."""
    print("\n" + "="*80)
    print("LABEL GENERATION RESULTS")
    print("="*80)
    
    data = result.get('data', {})
    
    input_mode = data.get('input_mode', 'unknown')
    print(f"\nInput Mode: {input_mode.upper()}")
    
    if input_mode == 'account':
        account_numbers = data.get('account_numbers', [])
        if len(account_numbers) == 1:
            print(f"Account Number: {account_numbers[0]}")
        else:
            print(f"Account Numbers: {', '.join(account_numbers)}")
        
        successful_accounts = data.get('successful_accounts', [])
        failed_accounts = data.get('failed_accounts', [])
        
        print(f"Accounts Processed: {len(successful_accounts)}/{len(account_numbers)} successful")
        if failed_accounts:
            print(f"Failed Accounts: {', '.join(failed_accounts)}")
        
        print(f"Lookback Period: {data.get('lookback_days')} days (from {data.get('cutoff_date')})")
        print(f"Minimum Order Count: {data.get('min_order_count', 2)}+")
        print(f"Total Unique Parts: {data.get('total_unique_parts', 'N/A')}")
        
        order_dist_path = data.get('order_distribution_path')
        if order_dist_path:
            print(f"Order Distribution Excel: {order_dist_path}")
        
        csv_path = data.get('scrape_info_sheet_path')
        if csv_path:
            print(f"Scrape Info Sheet CSV: {csv_path}")
    else:
        print(f"Excel File: {data.get('excel_file_path')}")
        customer_refs_count = data.get('customer_refs_count', 0)
        if customer_refs_count > 0:
            print(f"Customer References Found: {customer_refs_count}")
    
    print(f"Analysis Date: {data.get('analysis_timestamp')}")
    print(f"Label Sizes: {', '.join(data.get('label_sizes', []))}")
    
    print(f"\nParts Processed: {data.get('frequent_parts_count')}")
    component_count = data.get('component_parts_count', 0)
    if component_count > 0:
        print(f"Component Parts: {component_count}")
    print(f"Scraping - Success: {data.get('scrape_successful')}, Failed: {data.get('scrape_failed')}")
    
    print("\n" + "-"*80)
    print("RESULTS BY SIZE:")
    print("-"*80)
    
    for size, size_data in data.get('size_results', {}).items():
        print(f"\n{size.upper()}:")
        print(f"  Labels folder: {size_data.get('labels_folder')}")
        print(f"  PDF: {size_data.get('pdf_path')}")
        print(f"  In PDF: {size_data.get('labels_in_pdf')}, Excluded: {size_data.get('labels_excluded')}")
    
    failed_parts = data.get('failed_parts', [])
    if failed_parts:
        print("\n" + "="*80)
        print("FAILED PARTS SUMMARY")
        print("="*80)
        for fp in failed_parts:
            display = fp.get('display_part', fp.get('part_number', 'Unknown'))
            print(f"  {display}: {fp['reason']}")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    tool = CustomerPartsAnalyzerAndLabelGenerator()
    
    # ==================== ACCOUNT MODE EXAMPLE ====================
    params = {
        "account_number": "115328800",
        "lookback_days": 380,
        "min_order_count": 2,
        "output_folder": r"P:\Capacity Management\Management\Automation & Assistant Hub\QR Code\Testing Files\Test Outputs\Scrape 7",
        "label_size": "medium"  
    }
    
    print(f"Starting label generation for account(s): {params['account_number']}")
    print(f"Lookback period: {params['lookback_days']} days")
    print(f"Minimum order count: {params['min_order_count']}+")
    print(f"Output folder: {params['output_folder']}")
    print(f"Label size: {params['label_size']}")
    
    result = tool.run(params)
    
    if result.get("status") == "error":
        print(f"\nError: {result.get('message')}")
        exit(1)
    
    print_results(result)
