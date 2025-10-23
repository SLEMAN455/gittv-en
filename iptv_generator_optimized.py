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
# CONFIGURATION CHẤT LƯỢNG CAO - STRICT FILTERING
# =======================================================================================

SOURCES = {
    "tv": [
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

# THÔNG SỐ CHẤT LƯỢNG CAO
CHANNEL_CHECK_TIMEOUT = 5   
FETCH_TIMEOUT = 25          
MAX_CONCURRENT_CHECKS = 150
MAX_RETRIES = 1             

# YÊU CẦU PING NGHIÊM NGẶT HƠN (giảm 15% = chỉ chấp nhận kênh nhanh hơn)
MAX_ACCEPTABLE_PING_MS = 3400  # Giảm từ 4000ms xuống 3400ms (15% faster)
EXCELLENT_PING_MS = 1500       # Ping xuất sắc
GOOD_PING_MS = 2500            # Ping tốt

# YÊU CẦU CHẤT LƯỢNG: CHỈ LẤY >= 1080P
QUALITY_KEYWORDS_REQUIRED = ['1080', 'fhd', 'full hd', '1920', '4k', 'uhd', '2160']
QUALITY_KEYWORDS_EXCLUDE = ['720', 'hd', '480', 'sd', '360', '240']

# QUỐC GIA BỊ LOẠI BỎ
BLOCKED_COUNTRIES = [
    'bangladesh', 'bd', 'bangla',
    'belarus', 'by',
    'costa rica', 'cr',
    'india', 'indian', 'in',
    'mexico', 'mx', 'spanish',
    'lao', 'laos', 'la'
]

# BATCH SIZE
BATCH_SIZE = 200

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('iptv_generator.log', encoding='utf-8', mode='w')
    ]
)

# =======================================================================================
# ENHANCED CHANNEL CLASS WITH QUALITY DETECTION
# =======================================================================================

class IPTVChannel:
    """Enhanced channel class with quality filtering"""
    
    __slots__ = ['name', 'url', 'attributes', 'category', 'status', 'ping', 
                 'url_hash', 'name_normalized', 'country', 'quality_score']
    
    def __init__(self, name, url, attributes, category):
        self.name = self._clean_name(name)
        self.url = url.strip()
        self.attributes = attributes
        self.category = category
        self.status = 'unchecked'
        self.ping = float('inf')
        self.quality_score = 0
        
        # Pre-compute hashes
        self.url_hash = hashlib.md5(url.encode()).hexdigest()[:16]
        self.name_normalized = self._normalize_name(name)
        self.country = self._extract_country()
        
        self._ensure_required_attributes()
        self._calculate_quality_score()

    def _clean_name(self, name):
        name = re.sub(r'\s+', ' ', name.strip())
        name = re.sub(r'[^\w\s\-\+\.\(\)\[\]&]', '', name)
        return name[:80]

    def _normalize_name(self, name):
        normalized = re.sub(r'[^\w\s]', '', name.lower())
        normalized = re.sub(r'\b(hd|fhd|uhd|4k|1080p|720p|sd|live|tv|channel)\b', '', normalized)
        return re.sub(r'\s+', ' ', normalized).strip()

    def _extract_country(self):
        """Enhanced country extraction with blocking"""
        group = self.attributes.get('group-title', '').lower()
        name_lower = self.name.lower()
        
        # CHECK BLOCKED COUNTRIES FIRST
        for blocked in BLOCKED_COUNTRIES:
            if blocked in group or blocked in name_lower:
                return 'BLOCKED'
        
        # Country mapping
        country_map = {
            'usa': 'USA', 'us': 'USA', 'united states': 'USA',
            'uk': 'UK', 'united kingdom': 'UK', 'britain': 'UK',
            'canada': 'CA', 'canadian': 'CA',
            'australia': 'AU', 'aussie': 'AU',
            'france': 'FR', 'french': 'FR',
            'germany': 'DE', 'german': 'DE',
            'spain': 'ES', 'spanish': 'ES',
            'italy': 'IT', 'italian': 'IT',
            'netherlands': 'NL', 'dutch': 'NL',
            'portugal': 'PT', 'portuguese': 'PT',
        }
        
        for keyword, country in country_map.items():
            if keyword in group or keyword in name_lower:
                return country
        
        return 'INT'

    def _calculate_quality_score(self):
        """Calculate quality score based on resolution indicators"""
        text = f"{self.name} {self.attributes.get('group-title', '')}".lower()
        score = 50  # BASE SCORE: Giả định trung bình nếu không có thông tin
        
        # HIGH QUALITY INDICATORS (BONUS)
        if any(kw in text for kw in ['4k', 'uhd', '2160']):
            score = 100  # 4K/UHD - Chất lượng cao nhất
        elif any(kw in text for kw in ['1080', 'fhd', 'full hd', '1920']):
            score = 80   # 1080p/FHD - Chất lượng tốt
        
        # PENALTY FOR CLEARLY LOW QUALITY
        if any(kw in text for kw in QUALITY_KEYWORDS_EXCLUDE):
            score = 0  # Chắc chắn chất lượng thấp, loại bỏ
        
        # BONUS FOR HEVC/H265 (codec hiện đại)
        if any(kw in text for kw in ['hevc', 'h265', 'x265']):
            score += 15
        
        self.quality_score = max(0, min(score, 115))  # Giới hạn 0-115

    def is_high_quality(self):
        """Check if channel meets quality requirements"""
        # LOẠI BỎ nếu có tag chất lượng thấp rõ ràng (720p, 480p, SD...)
        if self.quality_score == 0:
            return False
        
        # LOẠI BỎ nếu từ quốc gia bị chặn
        if self.country == 'BLOCKED':
            return False
        
        # CHẤP NHẬN: Kênh có tag 1080p+ HOẶC không có tag gì (score=50)
        return True

    def _ensure_required_attributes(self):
        if 'tvg-id' not in self.attributes:
            self.attributes['tvg-id'] = re.sub(r'[^\w-]', '-', self.name.lower())[:40]
        if 'tvg-name' not in self.attributes:
            self.attributes['tvg-name'] = self.name
        if 'group-title' not in self.attributes:
            self.attributes['group-title'] = self.category.upper()

    def to_m3u_entry(self):
        attrs = []
        for key in ['tvg-id', 'tvg-name', 'group-title']:
            if key in self.attributes and self.attributes[key]:
                attrs.append(f'{key}="{self.attributes[key]}"')
        
        return f"#EXTINF:-1 {' '.join(attrs)},{self.name}\n{self.url}"

# =======================================================================================
# PARSING
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
            url = None
            for j in range(i + 1, min(i + 5, len(lines))):
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
# FETCHING
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
    """Enhanced channel checking with strict ping requirements"""
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
                ping_ms = (end_time - start_time) * 1000
                
                # STRICT PING REQUIREMENT
                if response.status in [200, 206, 301, 302, 303, 307, 308] and ping_ms <= MAX_ACCEPTABLE_PING_MS:
                    channel.status = 'working'
                    channel.ping = ping_ms
                else:
                    channel.status = 'dead'
                    
        except:
            channel.status = 'dead'

# =======================================================================================
# ENHANCED FILTERING WITH QUALITY CHECKS
# =======================================================================================

def filter_and_deduplicate(channels):
    """Enhanced filtering with quality and ping requirements"""
    logging.info(f"Starting quality filtering on {len(channels)} channels...")
    
    # STEP 1: Filter working channels
    working = [ch for ch in channels if ch.status == 'working']
    logging.info(f"Working channels: {len(working)}/{len(channels)}")
    
    if not working:
        return []
    
    # STEP 2: Filter by quality (loại bỏ kênh RÕ RÀNG chất lượng thấp hoặc bị chặn)
    high_quality = [ch for ch in working if ch.is_high_quality()]
    logging.info(f"After quality filter (removed low-quality tags & blocked countries): {len(high_quality)}/{len(working)}")
    
    if not high_quality:
        logging.warning("No channels passed quality filter!")
        return []
    
    # STEP 3: Filter by ping
    fast_channels = [ch for ch in high_quality if ch.ping <= MAX_ACCEPTABLE_PING_MS]
    logging.info(f"Fast enough (ping <= {MAX_ACCEPTABLE_PING_MS}ms): {len(fast_channels)}/{len(high_quality)}")
    
    if not fast_channels:
        return []
    
    # STEP 4: Deduplicate by URL (keep best ping)
    url_map = {}
    for ch in fast_channels:
        if ch.url_hash not in url_map or ch.ping < url_map[ch.url_hash].ping:
            url_map[ch.url_hash] = ch
    
    unique_urls = list(url_map.values())
    logging.info(f"After URL deduplication: {len(unique_urls)}")
    
    # STEP 5: Deduplicate by name per country (ƯU TIÊN chất lượng cao hơn)
    final_map = {}
    for ch in unique_urls:
        key = (ch.name_normalized, ch.country, ch.category)
        if key not in final_map:
            final_map[key] = ch
        else:
            existing = final_map[key]
            # Ưu tiên: Chất lượng cao hơn → Ping thấp hơn
            if (ch.quality_score > existing.quality_score or 
                (ch.quality_score == existing.quality_score and ch.ping < existing.ping)):
                final_map[key] = ch
    
    final = list(final_map.values())
    
    # STEP 6: Sort by quality and ping
    final.sort(key=lambda x: (x.category, -x.quality_score, x.ping))
    
    # Statistics
    uhd_4k = len([ch for ch in final if ch.quality_score >= 100])
    fhd_1080 = len([ch for ch in final if 80 <= ch.quality_score < 100])
    unknown = len([ch for ch in final if ch.quality_score == 50])
    enhanced = len([ch for ch in final if 50 < ch.quality_score < 80])
    
    logging.info(f"Final channels: {len(final)}")
    logging.info(f"  └─ 4K/UHD: {uhd_4k} | 1080p: {fhd_1080} | Unknown quality: {unknown} | Enhanced: {enhanced}")
    
    excellent = len([ch for ch in final if ch.ping <= EXCELLENT_PING_MS])
    good = len([ch for ch in final if EXCELLENT_PING_MS < ch.ping <= GOOD_PING_MS])
    acceptable = len([ch for ch in final if ch.ping > GOOD_PING_MS])
    
    logging.info(f"Ping breakdown - Excellent: {excellent}, Good: {good}, Acceptable: {acceptable}")
    
    return final

# =======================================================================================
# OUTPUT GENERATION
# =======================================================================================

def generate_m3u_playlist(channels):
    """Enhanced M3U generation with quality info"""
    logging.info("Generating high-quality M3U playlist...")
    
    lines = [
        '#EXTM3U',
        f'#EXTINF:-1,Optimized Playlist - Updated: {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}',
        f'#EXTINF:-1,Total: {len(channels)} channels (Prioritized: 1080p+, Fast Ping, No Low Quality)',
        ''
    ]
    
    # Group by category
    grouped = defaultdict(list)
    for ch in channels:
        grouped[ch.category].append(ch)
    
    for category in sorted(grouped.keys()):
        category_channels = grouped[category]
        avg_ping = sum(ch.ping for ch in category_channels) / len(category_channels)
        
        lines.append(f'#EXTINF:-1,━━━ {category.upper()} ({len(category_channels)} channels, avg {avg_ping:.0f}ms) ━━━')
        lines.append('')
        
        for ch in category_channels:
            lines.append(ch.to_m3u_entry())
            lines.append('')
    
    logging.info(f"Generated playlist with {len(channels)} optimized channels")
    
    return '\n'.join(lines)

# =======================================================================================
# MAIN
# =======================================================================================

async def main():
    """Main execution with enhanced quality filtering"""
    start_time = datetime.now()
    logging.info("=" * 60)
    logging.info("IPTV GENERATOR - OPTIMIZED QUALITY MODE")
    logging.info("Strategy: Prioritize 1080p+, Remove low-quality tags, Fast ping")
    logging.info("=" * 60)
    
    all_channels = []
    
    connector = aiohttp.TCPConnector(
        limit=200,
        limit_per_host=20,
        ttl_dns_cache=600,
        force_close=False,
    )
    
    async with aiohttp.ClientSession(
        connector=connector,
        headers=REQUEST_HEADERS,
        timeout=aiohttp.ClientTimeout(total=300)
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
        
        # Pre-filter by quality before checking (chỉ loại bỏ kênh RÕ RÀNG chất lượng thấp)
        all_channels = [ch for ch in all_channels if ch.is_high_quality()]
        logging.info(f"Channels after pre-filter (removed low-quality & blocked): {len(all_channels)}")
        
        if not all_channels:
            logging.error("No channels passed pre-filter (all are low-quality or blocked)!")
            return
        
        # Phase 2: Check channels
        logging.info("\n[2/3] CHECKING CHANNELS (Quality-Optimized)")
        logging.info("-" * 60)
        
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
        
        for i in range(0, len(all_channels), BATCH_SIZE):
            batch = all_channels[i:i+BATCH_SIZE]
            check_tasks = [check_channel_status(session, ch, semaphore) for ch in batch]
            await asyncio.gather(*check_tasks)
            
            checked = i + len(batch)
            if checked % (BATCH_SIZE * 5) == 0 or checked == len(all_channels):
                logging.info(f"Progress: {checked}/{len(all_channels)}")
    
    # Phase 3: Filter and generate
    logging.info("\n[3/3] GENERATING OPTIMIZED OUTPUT")
    logging.info("-" * 60)
    
    final_channels = filter_and_deduplicate(all_channels)
    
    if not final_channels:
        logging.error("No channels passed all filters!")
        return
    
    content = generate_m3u_playlist(final_channels)
    
    # Save
    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        f.write(content)
    
    duration = datetime.now() - start_time
    minutes = int(duration.total_seconds() / 60)
    seconds = int(duration.total_seconds() % 60)
    
    logging.info("\n" + "=" * 60)
    logging.info(f"✓✓✓ SUCCESS! Generated {len(final_channels)} OPTIMIZED channels")
    logging.info(f"Strategy: Prioritized 1080p+, removed low-quality, ping <={MAX_ACCEPTABLE_PING_MS}ms")
    logging.info(f"Execution time: {minutes}m {seconds}s")
    logging.info(f"Playlist saved to: {OUTPUT_FILENAME}")
    logging.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
