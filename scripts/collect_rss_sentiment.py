#!/usr/bin/env python3
"""
V5 RSS情报收集器 + DeepSeek情绪分析

抓取加密货币RSS源，提取文章内容，使用DeepSeek分析情绪
"""

import argparse
import os
import sys
import json
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from configs.runtime_config import resolve_runtime_env_path
from src.factors.deepseek_sentiment_factor import DeepSeekSentimentFactor


RSS_SOURCES = [
    {
        'name': 'CoinDesk',
        'url': 'https://www.coindesk.com/arc/outboundfeeds/rss/',
        'weight': 1.0,
        'enabled': True,
    },
    {
        'name': 'Cointelegraph',
        'url': 'https://cointelegraph.com/rss',
        'weight': 1.0,
        'enabled': True,
    },
    {
        'name': 'TheBlock',
        'url': 'https://www.theblock.co/rss.xml',
        'weight': 0.8,
        # The production host currently receives Cloudflare 403 responses.
        'enabled': False,
    },
]


def get_cache_dir(project_root: Path | None = None) -> Path:
    return (project_root or PROJECT_ROOT).resolve() / "data" / "sentiment_cache"


class MLStripper(HTMLParser):
    """去除HTML标签"""
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []
    
    def handle_data(self, d):
        self.fed.append(d)
    
    def get_data(self):
        return ''.join(self.fed)


def strip_tags(html):
    """去除HTML标签"""
    s = MLStripper()
    try:
        s.feed(html)
        return s.get_data()
    except:
        return html


def clean_text(text):
    """清理文本"""
    if not text:
        return ""
    # 去除多余空白
    text = re.sub(r'\s+', ' ', text)
    # 去除特殊字符
    text = re.sub(r'[^\w\s.,;:!?\-\(\)\[\]"\'@#$%&*]', '', text)
    return text.strip()


def parse_rss_feed(url: str, max_items: int = 5) -> list:
    """解析RSS feed，返回文章列表"""
    articles = []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        # 解析XML
        root = ET.fromstring(response.content)
        
        # 处理RSS 2.0和Atom格式
        items = []
        if root.tag == 'rss':
            channel = root.find('channel')
            if channel is not None:
                items = channel.findall('item')
        elif root.tag.endswith('feed'):  # Atom
            items = root.findall('{http://www.w3.org/2005/Atom}entry')
            if not items:
                items = root.findall('entry')
        
        for item in items[:max_items]:
            try:
                # RSS 2.0
                title = item.findtext('title', '')
                description = item.findtext('description', '')
                link = item.findtext('link', '')
                pub_date = item.findtext('pubDate', '')
                
                # Atom格式备选
                if not title:
                    title = item.findtext('{http://www.w3.org/2005/Atom}title', '')
                if not description:
                    desc_elem = item.find('{http://www.w3.org/2005/Atom}summary')
                    if desc_elem is not None:
                        description = desc_elem.text or ''
                if not link:
                    link_elem = item.find('{http://www.w3.org/2005/Atom}link')
                    if link_elem is not None:
                        link = link_elem.get('href', '')
                
                # 清理HTML标签
                title = clean_text(strip_tags(title))
                description = clean_text(strip_tags(description))[:500]  # 限制长度
                
                if title:  # 至少要有标题
                    articles.append({
                        'title': title,
                        'summary': description,
                        'link': link,
                        'published': pub_date,
                        'source': urlparse(url).netloc
                    })
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"[RSS] 解析 {url} 失败: {e}")
    
    return articles


def collect_rss_sentiment(*, env_path: str = ".env", project_root: Path | None = None):
    """收集RSS情报并进行情绪分析"""
    root = (project_root or PROJECT_ROOT).resolve()
    resolved_env_path = resolve_runtime_env_path(env_path, project_root=root)
    
    # RSS源配置
    rss_sources = [source for source in RSS_SOURCES if source.get('enabled', True)]
    
    cache_dir = get_cache_dir(root)
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H')
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始收集RSS情报...")
    
    all_articles = []
    for source in rss_sources:
        try:
            articles = parse_rss_feed(source['url'], max_items=5)
            for article in articles:
                article['source_weight'] = source['weight']
                article['source_name'] = source['name']
            all_articles.extend(articles)
            print(f"  {source['name']}: 获取 {len(articles)} 篇文章")
        except Exception as e:
            print(f"  {source['name']}: 失败 - {e}")
    
    if not all_articles:
        print("[RSS] 没有获取到任何文章")
        return
    
    print(f"[RSS] 总共获取 {len(all_articles)} 篇文章，开始情绪分析...")
    
    # 合并文本进行情绪分析
    # 按来源权重排序，优先分析高权重来源
    all_articles.sort(key=lambda x: x.get('source_weight', 1), reverse=True)
    
    # 准备分析文本（限制token数量）
    texts = []
    total_length = 0
    max_length = 8000  # 限制总长度，控制API成本
    
    for article in all_articles[:10]:  # 最多分析10篇
        text = f"[{article['source_name']}] {article['title']}"
        if article.get('summary'):
            text += f": {article['summary']}"
        
        if total_length + len(text) > max_length:
            break
        
        texts.append(text)
        total_length += len(text)
    
    # 使用DeepSeek分析情绪
    try:
        factor = DeepSeekSentimentFactor(
            cache_dir=str(cache_dir),
            env_path=resolved_env_path,
            project_root=root,
        )
        combined_text = "\n\n".join(texts)
        
        print(f"[RSS] 发送 {len(texts)} 篇文章到DeepSeek分析...")
        result = factor.analyze_sentiment([combined_text], symbol="MARKET")
        
        sentiment_score = result.get('sentiment_score', 0)
        fear_greed = result.get('fear_greed_index', 50)
        summary = result.get('summary', '')
        stage = result.get('market_stage', 'neutral')
        
        print(f"[RSS] 分析完成: 情绪={sentiment_score:.2f}, 阶段={stage}")
        print(f"[RSS] 摘要: {summary[:100]}...")
        
        # 保存结果（兼容f6_sentiment格式）
        cache_data = {
            'f6_sentiment': sentiment_score,
            'f6_sentiment_magnitude': abs(sentiment_score),
            'f6_fear_greed_index': fear_greed,
            'f6_sentiment_summary': f"[RSS情报] {summary}",
            'f6_sentiment_confidence': result.get('confidence', 0.7),
            'f6_sentiment_source': 'rss_deepseek',
            'f6_market_stage': stage,
            'rss_articles_count': len(all_articles),
            'rss_sources': list(set(a['source_name'] for a in all_articles)),
            'analyzed_texts': len(texts),
            'collected_at': datetime.now().isoformat()
        }
        
        # 保存为通用市场情绪文件
        cache_file = cache_dir / f"rss_MARKET_{timestamp}.json"
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        # 同时保存为各币种文件（复用）
        for symbol in ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT']:
            symbol_file = cache_dir / f"rss_{symbol}_{timestamp}.json"
            with open(symbol_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        
        print(f"[RSS] 情绪数据已保存到 {cache_file}")
        
    except Exception as e:
        print(f"[RSS] DeepSeek分析失败: {e}")
        import traceback
        traceback.print_exc()


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=".env")
    args = parser.parse_args(argv)
    collect_rss_sentiment(env_path=args.env)


if __name__ == '__main__':
    main()
