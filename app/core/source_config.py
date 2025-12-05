"""Configuration for per-source polling intervals and monitoring."""

from dataclasses import dataclass
from typing import Dict
import yaml
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class SourceConfig:
    """Configuration for a news source."""

    source_name: str
    enabled: bool = True
    max_articles: int = 30
    timeout_seconds: int = 10
    url: str = ""
    feed_url: str = ""  # RSS feed URL (for RSS sources)
    type: str = ""  # Source type: "rss", "reddit", "hackernews"

    def __repr__(self):
        return (
            f"SourceConfig({self.source_name}, "
            f"enabled={self.enabled}, "
            f"max_articles={self.max_articles})"
        )


def load_sources_config() -> Dict[str, SourceConfig]:
    """Load sources configuration from YAML file.
    
    Returns:
        Dictionary mapping source names to SourceConfig objects
    """
    config_path = Path(__file__).parent / "sources_config.yaml"
    
    if not config_path.exists():
        logger.warning(f"Sources config file not found at {config_path}, using defaults")
        return _get_default_configs()
    
    try:
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        sources_data = data.get("sources", {})
        configs = {}
        
        for source_name, source_data in sources_data.items():
            configs[source_name] = SourceConfig(
                source_name=source_name,
                enabled=source_data.get("enabled", True),
                max_articles=source_data.get("max_articles", 30),
                timeout_seconds=source_data.get("timeout_seconds", 10),
                url=source_data.get("url", ""),
                feed_url=source_data.get("feed_url", ""),
                type=source_data.get("type", ""),
            )
        
        logger.info(f"Loaded {len(configs)} source configurations from YAML")
        return configs
        
    except Exception as e:
        logger.error(f"Error loading sources config: {e}")
        return _get_default_configs()


def _get_default_configs() -> Dict[str, SourceConfig]:
    """Get default source configurations as fallback.
    
    Returns:
        Dictionary of default SourceConfig objects
    """
    return {
        "HackerNews": SourceConfig("HackerNews", enabled=True, max_articles=30),
        "TechCrunch": SourceConfig("TechCrunch", enabled=True, max_articles=30),
        "ArsTechnica": SourceConfig("ArsTechnica", enabled=True, max_articles=30),
        "TheVerge": SourceConfig("TheVerge", enabled=True, max_articles=30),
        "Slashdot": SourceConfig("Slashdot", enabled=True, max_articles=30),
        "Reddit-programming": SourceConfig("Reddit-programming", enabled=True, max_articles=25),
        "Reddit-cybersecurity": SourceConfig("Reddit-cybersecurity", enabled=True, max_articles=25),
        "Reddit-opensource": SourceConfig("Reddit-opensource", enabled=True, max_articles=25),
    }


# Load configurations on module import
SOURCE_CONFIGS = load_sources_config()


def get_source_config(source_name: str) -> SourceConfig:
    """Get configuration for a source, with defaults.

    Args:
        source_name: Name of the source

    Returns:
        SourceConfig for the source
    """
    return SOURCE_CONFIGS.get(
        source_name,
        SourceConfig(
            source_name=source_name,
            enabled=True,
            max_articles=30,
        ),
    )


def get_all_configs() -> Dict[str, SourceConfig]:
    """Get all source configurations.

    Returns:
        Dictionary mapping source names to their configs
    """
    return SOURCE_CONFIGS.copy()
