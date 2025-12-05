#!/usr/bin/env python
"""Inspect filter results for articles in the database."""

import json
import requests
from datetime import datetime
import sys

API_BASE = "http://localhost:8000/api/ingestion"

def inspect_filter_analysis():
    """Get comprehensive filter analysis."""
    print("\n" + "="*80)
    print("FILTER ANALYSIS DASHBOARD")
    print("="*80)
    
    try:
        response = requests.get(f"{API_BASE}/filter-analysis?limit=100")
        response.raise_for_status()
        data = response.json()
        
        # Print summary
        summary = data.get("summary", {})
        print(f"\n📊 Summary Statistics:")
        print(f"  Total Articles: {summary.get('total_articles', 0)}")
        print(f"  Passed: {summary.get('passed', 0)}")
        print(f"  Rejected: {summary.get('rejected', 0)}")
        print(f"  Pass Rate: {summary.get('pass_rate', 'N/A')}")
        print(f"  AI Enhanced: {summary.get('ai_enhanced_results', 0)}")
        
        # Print articles
        articles = data.get("articles", [])
        print(f"\n📄 Articles ({len(articles)} shown):")
        print("-" * 80)
        
        for i, article in enumerate(articles, 1):
            status = "✅ PASS" if article.get("passed_filter") else "❌ REJECT"
            print(f"\n{i}. {status} - {article.get('title', 'N/A')[:60]}")
            print(f"   ID: {article.get('id')}")
            print(f"   Source: {article.get('source')}")
            print(f"   Relevance Score: {article.get('relevance_score', 'N/A')}")
            print(f"   Quality Score: {article.get('quality_score', 'N/A')}")
            print(f"   AI Used: {article.get('ai_used', False)}")
            
            # Show filter details
            filters = article.get("filters", {})
            if filters:
                relevance_filter = filters.get("relevance", {})
                quality_filter = filters.get("quality", {})
                
                print(f"   Relevance: {relevance_filter.get('reason', 'N/A')}")
                print(f"   Quality: {quality_filter.get('reason', 'N/A')}")
        
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def inspect_article(article_id):
    """Get detailed filter results for a specific article."""
    print(f"\n" + "="*80)
    print(f"DETAILED FILTER RESULTS - Article: {article_id}")
    print("="*80)
    
    try:
        # Try as external_id first (string), then fall back to database ID (int)
        try:
            article_id_int = int(article_id)
            url = f"{API_BASE}/filter-results/{article_id_int}"
        except ValueError:
            # Not an integer, treat as external_id
            url = f"{API_BASE}/filter-results/by-external-id/{article_id}"
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        print(f"\n📋 Article: {data.get('title')}")
        print(f"   Database ID: {data.get('id')}")
        print(f"   External ID: {data.get('external_id', 'N/A')}")
        print(f"   Source: {data.get('source')}")
        print(f"   URL: {data.get('url')}")
        print(f"   Status: {'✅ PASSED' if data.get('passed_filter') else '❌ REJECTED'}")
        
        filter_details = data.get("filter_details", {})
        
        print(f"\n🔍 Filter Details:")
        print(f"   Overall Passed: {filter_details.get('passed')}")
        print(f"   Relevance Score: {filter_details.get('relevance_score')}")
        print(f"   Quality Score: {filter_details.get('quality_score')}")
        
        # Show filter breakdown
        filters = filter_details.get("filters", {})
        print(f"\n🏷️  Filters:")
        
        relevance = filters.get("relevance", {})
        print(f"   Relevance:")
        print(f"     - Passed: {relevance.get('passed')}")
        print(f"     - Reason: {relevance.get('reason')}")
        
        quality = filters.get("quality", {})
        print(f"   Quality:")
        print(f"     - Passed: {quality.get('passed')}")
        print(f"     - Reason: {quality.get('reason')}")
        
        # Show AI results if present
        ai = filter_details.get("ai")
        if ai:
            print(f"\n🤖 AI Classifier Results:")
            print(f"   Relevance Score: {ai.get('relevance_score')}")
            print(f"   Quality Score: {ai.get('quality_score')}")
            if ai.get('reasoning'):
                print(f"   Reasoning: {ai.get('reasoning')}")
        
        # Pretty print entire filter_details
        print(f"\n📊 Full Filter Details (JSON):")
        print(json.dumps(filter_details, indent=2))
        
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        article_id = sys.argv[1]
        print(f"Looking up article: {article_id}")
        print(f"(Accepts both database ID like '123' or external ID like 'event-123')")
        inspect_article(article_id)
    else:
        inspect_filter_analysis()


if __name__ == "__main__":
    main()
