"""
Stance Classifier - Classify article stance relative to cluster narrative
==========================================================================

Uses Mistral 7B Instruct via Hugging Face Inference API.
"""

import os
import json
from pathlib import Path
import requests


class StanceClassifier:
    """Classify article stance using Mistral 7B."""
    
    def __init__(self, test_mode=False):
        """Initialize classifier."""
        self.api_token = os.getenv("HUGGINGFACE_API_TOKEN")
        if not self.api_token:
            raise ValueError("HUGGINGFACE_API_TOKEN not set")
        
        self.model_id = "mistralai/Mistral-7B-Instruct-v0.3"
        self.api_url = f"https://api-inference.huggingface.co/models/{self.model_id}"
        self.test_mode = test_mode
        
        if test_mode:
            self.test_dir = Path("test_outputs/stances")
            self.test_dir.mkdir(parents=True, exist_ok=True)
            print(f"📁 Test mode: {self.test_dir}")
    
    def classify_stance(self, article: dict, cluster_summary: dict, cluster_id: str = "unknown") -> dict:
        """Classify article's stance."""
        article_title = article.get("title", "No title")
        
        if self.test_mode:
            print(f"\n{'='*80}\nCLASSIFYING STANCE\n{'='*80}")
            print(f"Cluster: {cluster_id}")
            print(f"Article: {article_title[:60]}...")
        
        prompt = self._build_classification_prompt(article, cluster_summary)
        
        if self.test_mode:
            self._save_test_file(cluster_id, article_title, "prompt.txt", prompt)
        
        try:
            if self.test_mode:
                print(f"\n🤖 Calling Mistral 7B...")
            
            response_text = self._call_mistral_api(prompt)
            
            if self.test_mode:
                self._save_test_file(cluster_id, article_title, "raw_response.txt", response_text)
                print(f"✅ Response: {len(response_text)} chars")
            
            stance_data = self._parse_classification_response(response_text)
            
            if self.test_mode:
                self._save_test_file(cluster_id, article_title, "parsed_stance.json",
                                   json.dumps(stance_data, indent=2))
                print(f"\n📊 {stance_data['classification'].upper()} (conf: {stance_data['confidence']:.2f})")
            
            return stance_data
        
        except Exception as e:
            print(f"❌ Error: {e}")
            if self.test_mode:
                self._save_test_file(cluster_id, article_title, "error.txt", str(e))
            return self._create_fallback_stance(str(e))
    
    def _build_classification_prompt(self, article: dict, cluster_summary: dict) -> str:
        """Build classification prompt."""
        title = article.get("title", "No title")
        body = article.get("body", "")[:1500]
        summary = cluster_summary.get("summary", "No summary")
        main_points = cluster_summary.get("main_points", [])
        
        points_text = "\n".join([f"  • {point}" for point in main_points])
        
        prompt = f"""[INST] Analyze how this article relates to a news story cluster.

CLUSTER NARRATIVE:
{summary}

KEY POINTS:
{points_text}

ARTICLE:
Title: {title}
Body: {body}

TASK: Classify the article's stance.

DEFINITIONS:
- SUPPORT: Agrees with, reinforces, or amplifies the narrative
- OPPOSE: Contradicts, challenges, or disputes the narrative
- NEUTRAL: Reports objectively without clear position

Provide JSON with:
1. "classification": "support" | "oppose" | "neutral"
2. "confidence": 0.0 to 1.0
3. "strength": "weak" | "moderate" | "strong"
4. "reasoning": 1-2 sentence explanation
5. "key_evidence": List of 1-3 quotes

Output ONLY valid JSON. [/INST]"""
        
        return prompt
    
    def _call_mistral_api(self, prompt: str, max_retries: int = 3) -> str:
        """Call Hugging Face API."""
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "inputs": prompt,
            "parameters": {
                "max_new_tokens": 400,
                "temperature": 0.2,
                "top_p": 0.9,
                "return_full_text": False
            }
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.post(self.api_url, headers=headers, json=payload, timeout=60)
                
                if response.status_code == 503:
                    print(f"⏳ Model loading, waiting 20s...")
                    import time
                    time.sleep(20)
                    continue
                
                response.raise_for_status()
                result = response.json()
                
                if isinstance(result, list) and len(result) > 0:
                    return result[0].get("generated_text", "")
                elif isinstance(result, dict):
                    return result.get("generated_text", "")
                else:
                    return str(result)
            
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"⚠️  Retry {attempt + 1}: {e}")
                    import time
                    time.sleep(5)
                else:
                    raise
        
        raise Exception("Max retries exceeded")
    
    def _parse_classification_response(self, response_text: str) -> dict:
        """Parse JSON response."""
        try:
            clean_text = response_text.strip()
            
            if "```json" in clean_text:
                start = clean_text.find("```json") + 7
                end = clean_text.find("```", start)
                clean_text = clean_text[start:end].strip()
            elif "```" in clean_text:
                start = clean_text.find("```") + 3
                end = clean_text.find("```", start)
                clean_text = clean_text[start:end].strip()
            
            if "{" in clean_text and "}" in clean_text:
                start = clean_text.find("{")
                end = clean_text.rfind("}") + 1
                clean_text = clean_text[start:end]
            
            data = json.loads(clean_text)
            
            valid_classes = ["support", "oppose", "neutral"]
            if data.get("classification") not in valid_classes:
                data["classification"] = "neutral"
            
            confidence = float(data.get("confidence", 0.5))
            data["confidence"] = max(0.0, min(1.0, confidence))
            
            valid_strengths = ["weak", "moderate", "strong"]
            if data.get("strength") not in valid_strengths:
                data["strength"] = "moderate"
            
            data.setdefault("reasoning", "No reasoning")
            data.setdefault("key_evidence", [])
            
            return data
        
        except Exception as e:
            print(f"⚠️  Parse error: {e}")
            return self._create_fallback_stance(f"Parse error: {e}")
    
    def _create_fallback_stance(self, reason: str) -> dict:
        """Create fallback stance."""
        return {
            "classification": "neutral",
            "confidence": 0.5,
            "strength": "weak",
            "reasoning": f"Classification failed: {reason}",
            "key_evidence": []
        }
    
    def _save_test_file(self, cluster_id: str, article_title: str, filename: str, content: str):
        """Save test output."""
        if not self.test_mode:
            return
        safe_title = "".join(c for c in article_title[:50] if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_title = safe_title.replace(' ', '_')
        cluster_dir = self.test_dir / cluster_id / safe_title
        cluster_dir.mkdir(parents=True, exist_ok=True)
        (cluster_dir / filename).write_text(content, encoding='utf-8')


def test_classifier():
    """Test the classifier."""
    print("="*80)
    print("TESTING STANCE CLASSIFIER (Mistral 7B)")
    print("="*80)
    
    test_summary = {
        "summary": "Microsoft announced the Maia 200 AI chip, claiming 3x performance improvements.",
        "main_points": ["3x inference performance", "Reduces NVIDIA reliance"]
    }
    
    test_articles = [
        {
            "title": "Microsoft's Custom Chip Strategy Pays Off",
            "body": "The Maia 200 represents a significant milestone. Early tests show impressive performance gains.",
            "expected_stance": "support"
        },
        {
            "title": "Experts Question Microsoft's Chip Performance Claims",
            "body": "While Microsoft touts 3x improvements, independent benchmarks have yet to verify these claims.",
            "expected_stance": "oppose"
        },
        {
            "title": "Microsoft Announces Maia 200 Chip",
            "body": "Microsoft has launched the Maia 200, a new AI chip for its Azure cloud platform.",
            "expected_stance": "neutral"
        }
    ]
    
    classifier = StanceClassifier(test_mode=True)
    
    results = []
    for i, test_article in enumerate(test_articles, 1):
        print(f"\n{'='*80}\nTEST CASE {i}/{len(test_articles)}\nExpected: {test_article['expected_stance'].upper()}\n{'='*80}")
        
        stance = classifier.classify_stance(
            article=test_article,
            cluster_summary=test_summary,
            cluster_id="TEST_product_launch_0_20260128"
        )
        
        results.append({
            "article": test_article["title"],
            "expected": test_article["expected_stance"],
            "actual": stance["classification"],
            "match": stance["classification"] == test_article["expected_stance"]
        })
    
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    
    for i, result in enumerate(results, 1):
        match_symbol = "✅" if result["match"] else "❌"
        print(f"\n{i}. {result['article'][:50]}...")
        print(f"   Expected: {result['expected'].upper()}")
        print(f"   Actual: {result['actual'].upper()}")
        print(f"   {match_symbol} {'MATCH' if result['match'] else 'MISMATCH'}")
    
    matches = sum(1 for r in results if r["match"])
    print(f"\n{'='*80}")
    print(f"Results: {matches}/{len(results)} correct ({matches/len(results)*100:.0f}%)")
    print(f"📁 Outputs: test_outputs/stances/TEST_product_launch_0_20260128/")
    
    return results


if __name__ == "__main__":
    test_classifier()