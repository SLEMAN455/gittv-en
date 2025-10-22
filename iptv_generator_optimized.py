# Tên tệp: iptv_generator_optimized.py

import asyncio
import aiohttp
import re
import logging
from datetime import datetime
from collections import defaultdict
from urllib.parse import urlparse
import hashlib
import sys

# =======================================================================================
# CONFIGURATION TỐI ƯU HÓA HIỆU SUẤT
# =======================================================================================

SOURCES = {
    "tv": [
        # GIỮ NGUYÊN TẤT CẢ 13 SOURCES GỐC
        "https://raw.githubusercontent.com/dishiptv/dish/main/stream.m3u",
        "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",
        "https://raw.githubusercontent.com/LS-Station/streamecho/main/StreamEcho.m3u8",
        "https://iptv-org.github.io/iptv/index.m3u",
        "https://raw.githubusercontent.com/binhex/iptv/main/eng.m3u",
        "https://raw.githubusercontent.com/serdartas/iptv-playlist/main/refined.m3u",
        "https://raw.githubusercontent.com/ipstreet312/freeiptv/master/all.m3u",
        "https://raw.githubusercontent.com/sultanarabi161/filoox-bdix/main/playlist.m3u",
        "https://raw.githubusercontent.com/Miraz6755/Iptv.m3u/main/DaddyLive.m3u",
        "https://raw.githubusercontent.com/AAAAAEXQOSyIpN2JZ0ehUQ/iPTV-FREE-LIST/master/iPTV-Free-List_TV.m3u",
        "https://raw.githubusercontent.com/dp247/IPTV/master/playlists/playlist_usa.m3u8",
        "https://raw.githubusercontent.com/dp247/IPTV/master/playlists/playlist_uk.m3u8",
        "https://raw.githubusercontent.com/HabibSay/free_iptv_m3u8/refs/heads/main/all_channels.m3u",
    ],
    "movies": [
        "https://aymrgknetzpucldhpkwm.supabase.co/storage/v1/object/public/tmdb/top-movies.m3u",
        "https://aymrgknetzpucldhpkwm.supabase.co/storage/v1/object/public/tmdb/action-movies.m3u",
        "https://aymrgknetzpucldhpkwm.supabase.co/storage/v1/object/public/tmdb/comedy-movies.m3u",
        "https://aymrgknetzpucldhpkwm.supabase.co/storage/v1/object/public/tmdb/horror-movies.m3u",
    ],
}

OUTPUT_FILENAME = "playlist.m3u"

# THÔNG SỐ TỐI ƯU HÓA THỜI GIAN CHẠY
CHANNEL_CHECK_TIMEOUT = 4   # GIẢM: Giảm từ 8 xuống 4 để giảm thời gian chờ kênh chết/lag
FETCH_TIMEOUT = 25          # Giữ nguyên, 25 giây là hợp lý
MAX_CONCURRENT_CHECKS = 100 # TĂNG: Tăng từ 40 lên 100 để kiểm tra song song nhiều hơn
MAX_RETRIES = 1             # Giữ nguyên, ít retry để chạy nhanh hơn

# BATCH SIZE (xử lý theo batch để tránh memory issues)
BATCH_SIZE = 200

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# LIGHTWEIGHT LOGGING (giảm log để chạy nhanh hơn)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('iptv_generator.log', encoding='utf-8', mode='w')  # Overwrite instead of append
    ]
)

# =======================================================================================
# LIGHTWEIGHT CHANNEL CLASS
# =======================================================================================

class IPTVChannel:
    """Lightweight channel class - optimized for speed"""
    
    __slots__ = ['name', 'url', 'attributes', 'category', 'status', 'ping', 
                 'url_hash', 'name_normalized', 'country']
    
    def __init__(self, name, url, attributes, category):
        self.name = self._clean_name(name)
        self.url = url.strip()
        self.attributes = attributes
        self.category = category
        self.status = 'unchecked'
        self.ping = float('inf')
        
        # Pre-compute hashes
        self.url_hash = hashlib.md5(url.encode()).hexdigest()[:16]  # Short hash
        self.name_normalized = self._normalize_name(name)
        self.country = self._extract_country()
        
        self._ensure_required_attributes()

    def _clean_name(self, name):
        name = re.sub(r'\s+', ' ', name.strip())
        name = re.sub(r'[^\w\s\-\+\.\(\)\[\]&]', '', name)
        return name[:80]  # Shorter limit

    def _normalize_name(self, name):
        normalized = re.sub(r'[^\w\s]', '', name.lower())
        normalized = re.sub(r'\b(hd|fhd|uhd|4k|1080p|720p|sd|live|tv|channel)\b', '', normalized)
        return re.sub(r'\s+', ' ', normalized).strip()

    def _extract_country(self):
        group = self.attributes.get('group-title', '').lower()
        
        # Simplified country mapping
        country_map = {
            'usa': 'USA', 'us': 'USA', 'uk': 'UK', 'canada': 'CA',
            'france': 'FR', 'germany': 'DE', 'spain': 'ES',
        }
        
        for keyword, country in country_map.items():
            if keyword in group:
                return country
        
        return 'INT'  # International

    def _ensure_required_attributes(self):
        if 'tvg-id' not in self.attributes:
            self.attributes['tvg-id'] = re.sub(r'[^\w-]', '-', self.name.lower())[:40]
        if 'tvg-name' not in self.attributes:
            self.attributes['tvg-name'] = self.name
        if 'group-title' not in self.attributes:
            self.attributes['group-title'] = self.category.upper()
        # Skip logo to save time

    def to_m3u_entry(self):
        attrs = []
        for key in ['tvg-id', 'tvg-name', 'group-title']:
            if key in self.attributes and self.attributes[key]:
                attrs.append(f'{key}="{self.attributes[key]}"')
        
        return f"#EXTINF:-1 {' '.join(attrs)},{self.name}\n{self.url}"

# =======================================================================================
# OPTIMIZED PARSING
# =======================================================================================

def parse_m3u_content(content, category):
    """Fast M3U parser"""
    channels = []
    content = content.replace('\r\n', '\n').replace('\r', '\n')
    if content.startswith('\ufeff'):
        content = content[1:]
    
    lines = content.split('\n')
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        if line.startswith('#EXTINF'):
            # Find URL
            url = None
            for j in range(i + 1, min(i + 5, len(lines))):  # Look ahead max 5 lines
                next_line = lines[j].strip()
                if next_line and not next_line.startswith('#'):
                    url = next_line
                    i = j
                    break
            
            if url and url.startswith('http'):
                try:
                    channel = parse_extinf_line(line, url, category)
                    if channel:
                        channels.append(channel)
                except:
                    pass
        
        i += 1
    
    return channels

def parse_extinf_line(extinf_line, url, category):
    """Fast EXTINF parser"""
    extinf_line = re.sub(r'^#EXTINF:-?\d+\s*', '', extinf_line)
    
    if ',' in extinf_line:
        attr_string, name = extinf_line.rsplit(',', 1)
    else:
        attr_string = extinf_line
        name = ''
    
    attributes = {}
    for match in re.finditer(r'([\w-]+)=(["\'])([^\2]*?)\2', attr_string):
        key, _, value = match.groups()
        attributes[key] = value
    
    if not name:
        name = attributes.get('tvg-name', attributes.get('tvg-id', ''))
    
    name = re.sub(r'[\w-]+=(["\'])[^\1]*\1', '', name).strip()
    
    if not name or len(name) < 2 or len(url) < 10:
        return None
    
    return IPTVChannel(name, url, attributes, category)

# =======================================================================================
# OPTIMIZED FETCHING
# =======================================================================================

async def fetch_source(session, url, category, retry=0):
    """Fast source fetching with minimal retries"""
    try:
        logging.info(f"Fetching: {url}")
        
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            allow_redirects=True
        ) as response:
            if response.status == 200:
                content = await response.text(errors='ignore')
                
                if '#EXTINF' not in content:
                    logging.warning(f"Invalid M3U: {url}")
                    return []
                
                channels = parse_m3u_content(content, category)
                logging.info(f"✓ {len(channels)} channels from {url}")
                return channels
            else:
                logging.warning(f"HTTP {response.status}: {url}")
                if retry < MAX_RETRIES:
                    await asyncio.sleep(1)
                    return await fetch_source(session, url, category, retry + 1)
                return []
                
    except asyncio.TimeoutError:
        logging.error(f"Timeout: {url}")
        return []
    except Exception as e:
        logging.error(f"Error: {url} - {str(e)[:50]}")
        return []

async def check_channel_status(session, channel, semaphore):
    """Fast channel checking"""
    async with semaphore:
        start_time = asyncio.get_event_loop().time()
        
        try:
            async with session.head(
                channel.url,
                timeout=aiohttp.ClientTimeout(total=CHANNEL_CHECK_TIMEOUT),
                headers=REQUEST_HEADERS,
                allow_redirects=True
            ) as response:
                end_time = asyncio.get_event_loop().time()
                
                # Accept more status codes to be lenient
                if response.status in [200, 206, 301, 302, 303, 307, 308, 403, 302]:
                    channel.status = 'working'
                    channel.ping = (end_time - start_time) * 1000
                else:
                    channel.status = 'dead'
                    
        except:
            channel.status = 'dead'

# =======================================================================================
# OPTIMIZED FILTERING
# =======================================================================================

def filter_and_deduplicate(channels):
    """Fast filtering and deduplication"""
    logging.info(f"Filtering {len(channels)} channels...")
    
    # Filter working
    working = [ch for ch in channels if ch.status == 'working']
    logging.info(f"Working: {len(working)}/{len(channels)}")
    
    if not working:
        return []
    
    # Deduplicate by URL (keep best ping)
    url_map = {}
    for ch in working:
        if ch.url_hash not in url_map or ch.ping < url_map[ch.url_hash].ping:
            url_map[ch.url_hash] = ch
    
    unique_urls = list(url_map.values())
    logging.info(f"After URL dedup: {len(unique_urls)}")
    
    # Deduplicate by name per country
    final_map = {}
    for ch in unique_urls:
        key = (ch.name_normalized, ch.country, ch.category)
        if key not in final_map or ch.ping < final_map[key].ping:
            final_map[key] = ch
    
    final = list(final_map.values())
    logging.info(f"Final: {len(final)} channels")
    
    # Sort
    final.sort(key=lambda x: (x.category, x.country, x.ping))
    
    return final

# =======================================================================================
# LIGHTWEIGHT OUTPUT
# =======================================================================================

def generate_m3u_playlist(channels):
    """Fast M3U generation"""
    logging.info("Generating M3U...")
    
    lines = [
        '#EXTM3U',
        f'#EXTINF:-1,Updated: {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}',
        ''
    ]
    
    # Group by category
    grouped = defaultdict(list)
    for ch in channels:
        grouped[ch.category].append(ch)
    
    for category in sorted(grouped.keys()):
        lines.append(f'#EXTINF:-1,━━━ {category.upper()} ━━━')
        lines.append('')
        
        for ch in grouped[category]:
            lines.append(ch.to_m3u_entry())
            lines.append('')
    
    logging.info(f"Generated playlist with {len(channels)} channels")
    
    return '\n'.join(lines)

# =======================================================================================
# OPTIMIZED MAIN
# =======================================================================================

async def main():
    """Optimized main execution"""
    start_time = datetime.now()
    logging.info("=" * 60)
    logging.info("IPTV GENERATOR - PERFORMANCE OPTIMIZED")
    logging.info("=" * 60)
    
    all_channels = []
    
    # Optimized connector
    connector = aiohttp.TCPConnector(
        limit=200,  # Tăng giới hạn tổng thể
        limit_per_host=20,
        ttl_dns_cache=600,
        force_close=False,
    )
    
    async with aiohttp.ClientSession(
        connector=connector,
        headers=REQUEST_HEADERS,
        timeout=aiohttp.ClientTimeout(total=300)  # 5 min global timeout
    ) as session:
        # Phase 1: Fetch sources
        logging.info("\n[1/3] FETCHING SOURCES")
        logging.info("-" * 60)
        
        tasks = []
        for category, urls in SOURCES.items():
            for url in urls:
                tasks.append(fetch_source(session, url, category))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, list):
                all_channels.extend(result)
        
        logging.info(f"Total parsed: {len(all_channels)}")
        
        if not all_channels:
            logging.error("No channels found from any source!")
            return
        
        # Phase 2: Check channels in batches
        logging.info("\n[2/3] CHECKING CHANNELS")
        logging.info("-" * 60)
        
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
        
        # Process in batches to avoid memory issues
        for i in range(0, len(all_channels), BATCH_SIZE):
            batch = all_channels[i:i+BATCH_SIZE]
            check_tasks = [check_channel_status(session, ch, semaphore) for ch in batch]
            await asyncio.gather(*check_tasks)
            
            checked = i + len(batch)
            if checked % (BATCH_SIZE * 5) == 0 or checked == len(all_channels):
                logging.info(f"Progress: {checked}/{len(all_channels)}")
    
    # Phase 3: Filter and generate
    logging.info("\n[3/3] GENERATING OUTPUT")
    logging.info("-" * 60)
    
    final_channels = filter_and_deduplicate(all_channels)
    
    if not final_channels:
        logging.error("No working channels found after filtering!")
        return
    
    content = generate_m3u_playlist(final_channels)
    
    # Save
    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        f.write(content)
    
    duration = datetime.now() - start_time
    minutes = int(duration.total_seconds() / 60)
    seconds = int(duration.total_seconds() % 60)
    
    logging.info("\n" + "=" * 60)
    logging.info(f"✓✓✓ SUCCESS! Generated {len(final_channels)} channels in {minutes}m {seconds}s")
    logging.info(f"Playlist saved to: {OUTPUT_FILENAME}")
    logging.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
