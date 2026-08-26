#!/usr/bin/env python3
"""
V5 RSS情报收集器 + DeepSeek情绪分析

抓取加密货币RSS源，提取文章内容，使用DeepSeek分析情绪
"""

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse

import requests

try:
    from defusedxml import ElementTree as SAFE_XML_ET
except ImportError:
    SAFE_XML_ET = None

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

MAX_ANALYSIS_ARTICLES = 8
MAX_ANALYSIS_CHARS = 3600
MAX_ARTICLE_SUMMARY_CHARS = 280
DEFAULT_MIN_REFRESH_MINUTES = 120
DEFAULT_UNCHANGED_RECHECK_MINUTES = 360
HIGH_IMPACT_TERMS = (
    "etf",
    "sec",
    "fed",
    "rate cut",
    "rate hike",
    "cpi",
    "inflation",
    "regulation",
    "ban",
    "hack",
    "exploit",
    "liquidation",
    "bankruptcy",
    "stablecoin",
    "tariff",
    "war",
)


def get_cache_dir(project_root: Path | None = None) -> Path:
    return (project_root or PROJECT_ROOT).resolve() / "data" / "sentiment_cache"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _article_identity(article: dict) -> str:
    link = str(article.get("link") or "").strip().lower().split("#", 1)[0]
    title = re.sub(r"\s+", " ", str(article.get("title") or "")).strip().lower()
    raw = link or title
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _article_impact_score(article: dict) -> tuple[float, str]:
    text = f"{article.get('title', '')} {article.get('summary', '')}".lower()
    impact = sum(1 for term in HIGH_IMPACT_TERMS if term in text)
    return float(article.get("source_weight", 1.0)) + impact * 0.5, str(
        article.get("published") or ""
    )


def _prepare_analysis_input(articles: list[dict]) -> tuple[list[str], list[str], str]:
    """Deduplicate and bound the exact text sent to the model."""
    unique: dict[str, dict] = {}
    for article in articles:
        identity = _article_identity(article)
        if identity not in unique:
            unique[identity] = article

    ranked = sorted(unique.items(), key=lambda item: _article_impact_score(item[1]), reverse=True)
    texts: list[str] = []
    article_ids: list[str] = []
    used_chars = 0
    for identity, article in ranked[:MAX_ANALYSIS_ARTICLES]:
        title = str(article.get("title") or "").strip()
        if not title:
            continue
        summary = str(article.get("summary") or "").strip()[:MAX_ARTICLE_SUMMARY_CHARS]
        source = str(article.get("source_name") or article.get("source") or "RSS").strip()
        text = f"{len(texts) + 1}|{source}|{title}"
        if summary:
            text += f"|{summary}"
        remaining = MAX_ANALYSIS_CHARS - used_chars
        if remaining <= 80:
            break
        if len(text) > remaining:
            text = text[:remaining]
        texts.append(text)
        article_ids.append(identity)
        used_chars += len(text) + 1

    fingerprint = hashlib.sha256("\n".join(article_ids).encode("utf-8")).hexdigest()
    return texts, article_ids, fingerprint


def _parse_utc(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _latest_valid_market_cache(cache_dir: Path) -> dict | None:
    for path in sorted(cache_dir.glob("rss_MARKET_*.json"), reverse=True):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        status = str(payload.get("deepseek_status") or "legacy_ok")
        source = str(payload.get("f6_sentiment_source") or "")
        summary = str(payload.get("f6_sentiment_summary") or "")
        confidence = float(payload.get("f6_sentiment_confidence") or 0.0)
        if (
            status in {"ok", "reused", "legacy_ok"}
            and source == "rss_deepseek"
            and "无数据" not in summary
            and confidence > 0.0
        ):
            return payload
    return None


def _has_high_impact_new_article(articles: list[dict], new_ids: set[str]) -> bool:
    for article in articles:
        if _article_identity(article) not in new_ids:
            continue
        text = f"{article.get('title', '')} {article.get('summary', '')}".lower()
        if any(term in text for term in HIGH_IMPACT_TERMS):
            return True
    return False


def _reuse_decision(
    previous: dict | None,
    *,
    article_ids: list[str],
    fingerprint: str,
    articles: list[dict],
    now: datetime,
) -> tuple[bool, str, int, float | None]:
    if not previous:
        return False, "no_previous_prediction", len(article_ids), None
    generated_at = _parse_utc(
        previous.get("analysis_generated_at") or previous.get("collected_at")
    )
    if generated_at is None:
        return False, "previous_prediction_time_unknown", len(article_ids), None

    age_minutes = max(0.0, (now - generated_at).total_seconds() / 60.0)
    previous_ids = {str(item) for item in previous.get("rss_article_ids") or []}
    new_ids = set(article_ids) - previous_ids
    new_count = len(new_ids)
    min_refresh = max(
        30,
        int(os.getenv("DEEPSEEK_RSS_MIN_REFRESH_MINUTES", str(DEFAULT_MIN_REFRESH_MINUTES))),
    )
    max_reuse = max(
        min_refresh,
        int(
            os.getenv(
                "DEEPSEEK_RSS_UNCHANGED_RECHECK_MINUTES",
                str(DEFAULT_UNCHANGED_RECHECK_MINUTES),
            )
        ),
    )

    if age_minutes < max_reuse and fingerprint == previous.get("rss_input_fingerprint"):
        return True, "unchanged_articles", new_count, age_minutes
    if (
        age_minutes < min_refresh
        and new_count < 2
        and not _has_high_impact_new_article(articles, new_ids)
    ):
        return True, "insufficient_new_market_information", new_count, age_minutes
    return False, "refresh_required", new_count, age_minutes


def _write_json_atomic(path: Path, payload: dict) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    temp_path.replace(path)


def _write_cache_bundle(cache_dir: Path, timestamp: str, payload: dict) -> Path:
    cache_file = cache_dir / f"rss_MARKET_{timestamp}.json"
    _write_json_atomic(cache_file, payload)
    for symbol in ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT']:
        _write_json_atomic(cache_dir / f"rss_{symbol}_{timestamp}.json", payload)
    return cache_file


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
    except Exception:
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


def _parse_rss_xml(content: bytes):
    if SAFE_XML_ET is None:
        raise RuntimeError("defusedxml is required for safe RSS parsing; install requirements.txt")
    return SAFE_XML_ET.fromstring(content)


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
        root = _parse_rss_xml(response.content)
        
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
            except Exception:
                continue
                
    except Exception as e:
        print(f"[RSS] 解析 {url} 失败: {e}")
    
    return articles


def collect_rss_sentiment(*, env_path: str = ".env", project_root: Path | None = None) -> bool:
    """收集RSS情报并进行情绪分析"""
    root = (project_root or PROJECT_ROOT).resolve()
    resolved_env_path = resolve_runtime_env_path(env_path, project_root=root)
    if SAFE_XML_ET is None:
        print("[RSS] 缺少 defusedxml，无法安全解析 RSS XML；请运行 pip install -r requirements.txt")
        return False

    # RSS源配置
    rss_sources = [source for source in RSS_SOURCES if source.get('enabled', True)]
    
    cache_dir = get_cache_dir(root)
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = _utc_now().strftime('%Y%m%d_%H')

    print(f"[{_utc_now().strftime('%Y-%m-%d %H:%M:%S')}] 开始收集RSS情报...")
    
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
        return False
    
    print(f"[RSS] 总共获取 {len(all_articles)} 篇文章，开始情绪分析...")
    
    texts, article_ids, fingerprint = _prepare_analysis_input(all_articles)
    if not texts:
        print("[RSS] 去重后没有可分析的文章")
        return False

    now = _utc_now()

    # 使用DeepSeek分析预测；构造实例也会加载生产 runtime env。
    try:
        factor = DeepSeekSentimentFactor(
            cache_dir=str(cache_dir),
            env_path=resolved_env_path,
            project_root=root,
        )
        previous = _latest_valid_market_cache(cache_dir)
        should_reuse, reuse_reason, new_article_count, analysis_age_minutes = _reuse_decision(
            previous,
            article_ids=article_ids,
            fingerprint=fingerprint,
            articles=all_articles,
            now=now,
        )
        if should_reuse and previous is not None:
            cache_data = dict(previous)
            cache_data.update(
                {
                    "rss_articles_count": len(all_articles),
                    "rss_sources": sorted({a['source_name'] for a in all_articles}),
                    "analyzed_texts": len(texts),
                    "rss_article_ids": article_ids,
                    "rss_input_fingerprint": fingerprint,
                    "rss_new_article_count": new_article_count,
                    "rss_input_chars": 0,
                    "analysis_input_chars": previous.get("analysis_input_chars")
                    or previous.get("rss_input_chars")
                    or 0,
                    "deepseek_status": "reused",
                    "deepseek_usage": {
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                    },
                    "deepseek_request_id": None,
                    "analysis_deepseek_usage": previous.get("analysis_deepseek_usage")
                    or previous.get("deepseek_usage")
                    or {},
                    "analysis_deepseek_request_id": previous.get(
                        "analysis_deepseek_request_id"
                    )
                    or previous.get("deepseek_request_id"),
                    "analysis_reused": True,
                    "reuse_reason": reuse_reason,
                    "previous_analysis_age_minutes": round(analysis_age_minutes or 0.0, 1),
                    "collected_at": _utc_now_iso(),
                }
            )
            cache_file = _write_cache_bundle(cache_dir, timestamp, cache_data)
            print(
                "[RSS] 复用最近预测: "
                f"reason={reuse_reason}, new_articles={new_article_count}, "
                f"analysis_age_minutes={analysis_age_minutes:.1f}"
            )
            print(f"[RSS] 预测缓存已保存到 {cache_file}")
            return True

        combined_text = "\n\n".join(texts)

        print(
            f"[RSS] 发送 {len(texts)} 篇去重新闻到DeepSeek预测... "
            f"input_chars={len(combined_text)}, new_articles={new_article_count}"
        )
        result = factor.analyze_sentiment([combined_text], symbol="MARKET")
        if not result.get("ok", True):
            print(
                "[RSS] DeepSeek预测不可用: "
                f"code={result.get('error_code')}, error={result.get('error')}"
            )
            return False

        sentiment_score = result.get('sentiment_score', 0)
        fear_greed = result.get('fear_greed_index', 50)
        summary = result.get('summary', '')
        stage = result.get('market_stage', 'neutral')

        print(
            f"[RSS] 预测完成: 方向={result.get('direction', 'flat')}, "
            f"情绪={sentiment_score:.2f}, 阶段={stage}, "
            f"usage={json.dumps(result.get('usage') or {}, ensure_ascii=False)}"
        )
        print(f"[RSS] 摘要: {summary[:100]}...")

        # 保存结果（兼容既有 f6_sentiment 字段，并增加短周期预测审计字段）
        cache_data = {
            'f6_sentiment': sentiment_score,
            'f6_sentiment_magnitude': abs(sentiment_score),
            'f6_fear_greed_index': fear_greed,
            'f6_sentiment_summary': f"[RSS预测4h] {summary}",
            'f6_sentiment_confidence': result.get('confidence', 0.7),
            'f6_sentiment_source': 'rss_deepseek',
            'f6_market_stage': stage,
            'f6_forecast_direction': result.get('direction', 'flat'),
            'f6_forecast_horizon_hours': result.get('horizon_hours', 4),
            'f6_up_probability': result.get('up_probability', 0.0),
            'f6_down_probability': result.get('down_probability', 0.0),
            'f6_flat_probability': result.get('flat_probability', 1.0),
            'f6_expected_move_bps': result.get('expected_move_bps', 0.0),
            'f6_forecast_catalysts': result.get('catalysts', []),
            'f6_forecast_risk_flags': result.get('risk_flags', []),
            'rss_articles_count': len(all_articles),
            'rss_sources': sorted({a['source_name'] for a in all_articles}),
            'analyzed_texts': len(texts),
            'rss_article_ids': article_ids,
            'rss_input_fingerprint': fingerprint,
            'rss_new_article_count': new_article_count,
            'rss_input_chars': len(combined_text),
            'analysis_input_chars': len(combined_text),
            'deepseek_status': 'ok',
            'deepseek_model': result.get('model') or getattr(factor, 'model', 'deepseek-chat'),
            'deepseek_usage': result.get('usage') or {},
            'deepseek_request_id': result.get('request_id'),
            'analysis_deepseek_usage': result.get('usage') or {},
            'analysis_deepseek_request_id': result.get('request_id'),
            'analysis_reused': False,
            'reuse_reason': None,
            'analysis_generated_at': _utc_now_iso(),
            'collected_at': _utc_now_iso(),
        }

        cache_file = _write_cache_bundle(cache_dir, timestamp, cache_data)
        print(f"[RSS] 预测缓存已保存到 {cache_file}")
        return True

    except Exception as e:
        print(f"[RSS] DeepSeek分析失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=".env")
    args = parser.parse_args(argv)
    if not collect_rss_sentiment(env_path=args.env):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
