"""Configuration loader for La Anónima Price Tracker."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from YAML file.
    
    Args:
        config_path: Path to config file. If None, uses default locations.
        
    Returns:
        Dictionary with configuration values.
    """
    # Load environment variables first
    load_dotenv()
    
    # Find config file
    if config_path is None:
        # Check common locations
        locations = [
            "config.yaml",
            "config.yml",
            "../config.yaml",
            "../config.yml",
            "/app/config.yaml",
        ]
        for loc in locations:
            if Path(loc).exists():
                config_path = loc
                break
    
    if config_path is None or not Path(config_path).exists():
        raise FileNotFoundError("Configuration file not found. Please provide config.yaml")
    
    # Load YAML
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    # Substitute environment variables
    config = _substitute_env_vars(config)
    
    return config


def _substitute_env_vars(obj: Any) -> Any:
    """Recursively substitute environment variables in config.
    
    Supports syntax: ${VAR_NAME} or ${VAR_NAME:default_value}
    """
    if isinstance(obj, dict):
        return {k: _substitute_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_substitute_env_vars(item) for item in obj]
    elif isinstance(obj, str):
        return _substitute_env_string(obj)
    else:
        return obj


def _substitute_env_string(value: str) -> str:
    """Substitute environment variables in a string."""
    import re
    
    pattern = r'\$\{([^}]+)\}'
    
    def replace(match):
        var_expr = match.group(1)
        if ':' in var_expr:
            var_name, default = var_expr.split(':', 1)
            return os.getenv(var_name, default)
        else:
            return os.getenv(var_expr, match.group(0))
    
    return re.sub(pattern, replace, value)


def get_basket_items(config: Dict[str, Any], basket_type: str = "cba") -> List[Dict[str, Any]]:
    """Get basket items from configuration.
    
    Args:
        config: Configuration dictionary
        basket_type: Type of basket ('cba', 'extended', or 'all')
        
    Returns:
        List of basket items with their configuration
    """
    baskets = config.get("baskets", {})
    items = []
    
    if basket_type in ["cba", "all"]:
        cba_items = baskets.get("cba", {}).get("items", [])
        for item in cba_items:
            item_copy = item.copy()
            item_copy["basket_type"] = "cba"
            items.append(item_copy)
    
    if basket_type in ["extended", "all"]:
        ext_items = baskets.get("extended", {}).get("items", [])
        for item in ext_items:
            item_copy = item.copy()
            item_copy["basket_type"] = "extended"
            items.append(item_copy)
    
    return items


def get_branch_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Get branch configuration."""
    return config.get("branch", {})


def get_scraping_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Get scraping configuration."""
    return config.get("scraping", {})


def get_storage_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Get storage configuration."""
    return config.get("storage", {})


def normalize_category_value(value: Optional[str]) -> str:
    """Normalize category labels for reliable matching."""
    if not value:
        return ""

    normalized = value.strip().lower()
    normalized = normalized.replace("á", "a").replace("é", "e").replace("í", "i")
    normalized = normalized.replace("ó", "o").replace("ú", "u").replace("ü", "u")
    return " ".join(normalized.split())


def get_canonical_category_map(config: Dict[str, Any]) -> Dict[str, str]:
    """Build alias -> canonical category map from config."""
    category_cfg = config.get("canonical_categories", {})
    aliases_cfg = category_cfg.get("aliases", {})

    category_map = {}
    for canonical, aliases in aliases_cfg.items():
        category_map[normalize_category_value(canonical)] = canonical
        for alias in aliases or []:
            category_map[normalize_category_value(alias)] = canonical

    return category_map


def get_category_display_names(config: Dict[str, Any]) -> Dict[str, str]:
    """Get canonical slug -> display name mapping."""
    return config.get("canonical_categories", {}).get("labels", {})


def resolve_canonical_category(config: Dict[str, Any], value: Optional[str]) -> Optional[str]:
    """Resolve current/raw category into canonical slug."""
    normalized = normalize_category_value(value)
    if not normalized:
        return None

    return get_canonical_category_map(config).get(normalized)


def ensure_directories(config: Dict[str, Any]):
    """Ensure all required directories exist."""
    storage = config.get("storage", {})
    
    # SQLite directory
    sqlite_path = storage.get("sqlite", {}).get("database_path", "data/prices.db")
    Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Export directories
    csv_path = storage.get("exports", {}).get("csv_path", "data/exports/csv")
    Path(csv_path).mkdir(parents=True, exist_ok=True)
    
    parquet_path = storage.get("exports", {}).get("parquet_path", "data/exports/parquet")
    Path(parquet_path).mkdir(parents=True, exist_ok=True)
    
    # Log directory
    log_path = config.get("logging", {}).get("file", "data/logs/tracker.log")
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Analysis directory
    analysis_path = config.get("analysis", {}).get("output_dir", "data/analysis")
    Path(analysis_path).mkdir(parents=True, exist_ok=True)
    
    plots_path = config.get("analysis", {}).get("plots_dir", "data/analysis/plots")
    Path(plots_path).mkdir(parents=True, exist_ok=True)
