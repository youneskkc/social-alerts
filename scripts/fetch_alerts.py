#!/usr/bin/env python3
"""Fetch social media posts matching keywords from Bluesky and Mastodon."""
import asyncio
import aiohttp
import json
import os
import re
import html
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(SCRIPT_DIR)
CONFIG_PATH = os.path.join(REPO_DIR, 'config.json')
OUTPUT_PATH = os.path.join(REPO_DIR, 'data.json')
HISTORY_PATH = os.path.join(REPO_DIR, 'history.json')

TIMEOUT = 20
MAX_CONCURRENT = 10
HOURS_BACK = 48

def clean_text(text):
    """Clean HTML and normalize whitespace."""
    if not text:
        return ''
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def post_id(post):
    """Generate a unique ID for deduplication."""
    return post.get('url', '') or f"{post.get('author','')}-{post.get('text','')[:50]}"

# ── Bluesky ──────────────────────────────────────────────

async def search_bluesky(session, keyword, config):
    """Search Bluesky for a keyword."""
    posts = []
    api = config['api']
    limit = config.get('limit_per_query', 30)
    try:
        params = {
            'q': keyword,
            'limit': str(limit),
            'sort': 'latest'
        }
        headers = {
            'User-Agent': 'SocialAlerts/1.0',
            'Accept': 'application/json'
        }
        url = f"{api}?q={quote(keyword)}&limit={limit}&sort=latest"
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)

        async with session.get(url, timeout=timeout, headers=headers, ssl=False) as resp:
            if resp.status == 200:
                data = await resp.json()
                for p in data.get('posts', []):
                    record = p.get('record', {})
                    author = p.get('author', {})
                    created = record.get('createdAt', '')
                    text = clean_text(record.get('text', ''))

                    # Build post URL
                    handle = author.get('handle', '')
                    uri = p.get('uri', '')
                    rkey = uri.split('/')[-1] if '/' in uri else ''
                    post_url = f"https://bsky.app/profile/{handle}/post/{rkey}" if handle and rkey else ''

                    posts.append({
                        'platform': 'bluesky',
                        'author': author.get('displayName', '') or handle,
                        'handle': f"@{handle}",
                        'avatar': author.get('avatar', ''),
                        'text': text,
                        'url': post_url,
                        'date': created,
                        'keyword': keyword,
                        'likes': p.get('likeCount', 0),
                        'reposts': p.get('repostCount', 0),
                    })
            else:
                print(f"  Bluesky [{keyword}]: HTTP {resp.status}")
    except Exception as e:
        print(f"  Bluesky [{keyword}]: Error - {str(e)[:60]}")
    return posts

# ── Mastodon ─────────────────────────────────────────────

async def search_mastodon_instance(session, instance, keyword, limit):
    """Search a single Mastodon instance."""
    posts = []
    try:
        url = f"{instance}/api/v2/search?q={quote(keyword)}&type=statuses&limit={limit}"
        headers = {'User-Agent': 'SocialAlerts/1.0'}
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)

        async with session.get(url, timeout=timeout, headers=headers, ssl=False) as resp:
            if resp.status == 200:
                data = await resp.json()
                for s in data.get('statuses', []):
                    account = s.get('account', {})
                    text = clean_text(s.get('content', ''))
                    posts.append({
                        'platform': 'mastodon',
                        'instance': instance.replace('https://', ''),
                        'author': account.get('display_name', '') or account.get('username', ''),
                        'handle': f"@{account.get('acct', '')}",
                        'avatar': account.get('avatar_static', ''),
                        'text': text,
                        'url': s.get('url', ''),
                        'date': s.get('created_at', ''),
                        'keyword': keyword,
                        'likes': s.get('favourites_count', 0),
                        'reposts': s.get('reblogs_count', 0),
                    })
            else:
                # Mastodon search often requires auth, silently skip
                pass
    except Exception:
        pass
    return posts

async def search_mastodon(session, keyword, config):
    """Search across multiple Mastodon instances."""
    instances = config.get('instances', ['https://mastodon.social'])
    limit = config.get('limit_per_query', 20)
    tasks = [search_mastodon_instance(session, inst, keyword, limit) for inst in instances]
    results = await asyncio.gather(*tasks)
    all_posts = []
    for r in results:
        all_posts.extend(r)
    return all_posts

# ── Main ─────────────────────────────────────────────────

async def main():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config = json.load(f)

    keywords = config['keywords']
    sources = config['sources']
    cutoff = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)

    print(f"Searching for {len(keywords)} keywords across open social platforms...")
    print(f"Cutoff: {cutoff.isoformat()}")

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
    all_posts = []

    async with aiohttp.ClientSession(connector=connector) as session:
        for keyword in keywords:
            print(f"\n  Keyword: {keyword}")

            if sources.get('bluesky', {}).get('enabled'):
                posts = await search_bluesky(session, keyword, sources['bluesky'])
                print(f"    Bluesky: {len(posts)} posts")
                all_posts.extend(posts)

            if sources.get('mastodon', {}).get('enabled'):
                posts = await search_mastodon(session, keyword, sources['mastodon'])
                print(f"    Mastodon: {len(posts)} posts")
                all_posts.extend(posts)

            # Be polite
            await asyncio.sleep(0.5)

    # Filter by date
    filtered = []
    for post in all_posts:
        if post['date']:
            try:
                dt = datetime.fromisoformat(post['date'].replace('Z', '+00:00'))
                if dt >= cutoff:
                    filtered.append(post)
            except:
                filtered.append(post)
        else:
            filtered.append(post)

    # Deduplicate by URL
    seen = set()
    unique = []
    for post in filtered:
        pid = post_id(post)
        if pid not in seen:
            seen.add(pid)
            unique.append(post)

    # Sort by date descending
    def sort_key(p):
        try:
            return datetime.fromisoformat(p['date'].replace('Z', '+00:00')).timestamp()
        except:
            return 0
    unique.sort(key=sort_key, reverse=True)

    # Load history for accumulation (keep 7 days)
    history = []
    if os.path.exists(HISTORY_PATH):
        try:
            with open(HISTORY_PATH, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except:
            history = []

    # Merge new with history
    history_urls = {post_id(p) for p in history}
    new_count = 0
    for post in unique:
        pid = post_id(post)
        if pid not in history_urls:
            history.append(post)
            history_urls.add(pid)
            new_count += 1

    # Trim history to 7 days
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    history = [p for p in history if (p.get('date', '') or '') >= week_ago or not p.get('date')]
    history.sort(key=sort_key, reverse=True)

    # Group by keyword for display
    by_keyword = {}
    for post in history:
        kw = post.get('keyword', 'أخرى')
        if kw not in by_keyword:
            by_keyword[kw] = []
        by_keyword[kw].append(post)

    # Also group by platform stats
    platform_stats = {}
    for post in history:
        plat = post.get('platform', 'unknown')
        platform_stats[plat] = platform_stats.get(plat, 0) + 1

    output = {
        'lastUpdated': datetime.now(timezone.utc).isoformat(),
        'totalPosts': len(history),
        'newPosts': new_count,
        'platformStats': platform_stats,
        'posts': history,
        'byKeyword': by_keyword
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False)

    with open(HISTORY_PATH, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False)

    print(f"\nTotal: {len(history)} posts ({new_count} new)")
    for plat, count in platform_stats.items():
        print(f"  {plat}: {count}")

if __name__ == '__main__':
    asyncio.run(main())
