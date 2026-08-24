"""
run_full_pipeline.py
=====================
Single-script merge of the 4-stage endpoint reachability pipeline:

  1) endpoint_capture_v7.py            -> Playwright crawl, capture endpoint URLs per site
  2) Doublecheck_endpoint1 2.py        -> re-classify / correct endpoint types
  3) filter_endpoint_with_recovery.py  -> pick best URL per type (+ recover missing types)
  4) gp_parallel_tokens11.ps1          -> Globalping ping+http measurement (translated to Python)

Runs the whole thing once, end to end, for manual testing. Data is passed
in-memory between stages (no intermediate CSVs) — only the final Globalping
measurement results and failures are written to disk.

Run (batch, from a CSV of sites to discover):
    python run_full_pipeline.py

Run (single site, e.g. from a web form / another backend):
    python run_full_pipeline.py --url myapp.com --country "India" --category "Ecommerce"

Run (a CSV of already-chosen endpoints — skips discovery entirely):
    python run_full_pipeline.py --endpoints-csv my_endpoints.csv

    Use this when the caller already knows the exact URLs to test (e.g. the
    "upload your own endpoints" path on a form) — columns: country,
    benchmark_category, recommended_brand_app, source_domain, endpoint_type,
    host, url. endpoint_type can be anything, including custom labels; it's
    only carried through to the output, never filtered on. Goes straight to
    stage 4 — no crawl, no reclassification, no filtering.

    All three modes print "RESULT_CSV=<path>" / "FAILURES_CSV=<path>" at the
    end so a calling backend (in any language) can locate the output by
    parsing stdout. If the caller is itself Python, prefer importing
    run_pipeline() / run_measurement_only() / build_single_site_input()
    directly instead of shelling out.

Requirements:
    pip install playwright pandas requests playwright-stealth
    playwright install chromium
"""

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import argparse
import os
import pandas as pd
import re
import requests
import time
import json

# =========================================================================
# CONFIG
# =========================================================================

SCRIPT_DIR = Path(__file__).resolve().parent

# Input CSV must have (at least) a URL column — see COLUMN_ALIASES below for
# accepted header names — plus optional country / benchmark_category /
# recommended_brand_app columns. Drop your file in this folder as input.csv,
# or point this at another path.
INPUT_CSV = SCRIPT_DIR / "input.csv"

RESULTS_CSV  = SCRIPT_DIR / "globalping_results_final.csv"
FAILURES_CSV = SCRIPT_DIR / "globalping_failures.csv"

# ---- Stage 1: Playwright capture ----
HEADLESS          = False    # no visible browser window; relies on stealth patching to avoid bot detection
PAGE_TIMEOUT_MS   = 50000
POST_LOAD_WAIT    = 5
SCROLL_PAUSE      = 0.4
CONSENT_WAIT      = 2
LOGIN_WAIT        = 6
SEARCH_WAIT       = 8
SEARCH_ENTER_WAIT = 6
SEARCH_QUERY      = "phone"
DEBUG             = True

SITE_OVERRIDES = {
    "alahli.com":           {"search_url": "https://www.alahli.com/en/pages/search"},
    "ncb.com.sa":           {"search_url": "https://www.alahli.com/en/pages/search"},
    "riyadbank.com":        {"search_url": "https://www.riyadbank.com/en/search"},
    "alahlionline.com":     {"login_url": "https://new.alahlionline.com/ui/"},
    "new.alahlionline.com": {"login_url": "https://new.alahlionline.com/ui/"},
}

KEEP_TYPES = {
    "main_page",
    "authentication_endpoint",
    "search_endpoint",
    "api_endpoint",
    "static_cdn_asset",
    "image",
    "static_asset",
    "analytics_tracking",
    "ad_endpoint",
    "waf_challenge",
    "payment_checkout",
    "localization_currency",
    "third_party_search",
    "feature_flag",
    "consent_management",
    "rum_telemetry",
    "affiliate_referral",
}

CAPTURE_COLUMNS = [
    "country", "benchmark_category", "recommended_brand_app",
    "source_domain", "endpoint_type", "host", "url",
]

PRIORITY = {
    "main_page":               100,
    "authentication_endpoint":  80,
    "payment_checkout":         75,
    "search_endpoint":          70,
    "third_party_search":       65,
    "waf_challenge":            60,
    "localization_currency":    45,
    "static_cdn_asset":         40,
    "image":                    35,
    "static_asset":             30,
    "api_endpoint":             20,
    "feature_flag":             15,
    "consent_management":       15,
    "affiliate_referral":       15,
    "analytics_tracking":       10,
    "ad_endpoint":              10,
    "rum_telemetry":            10,
}

# ---- Stage 3: filter + recovery limits ----
LIMITS = {
    "main_page": 1,
    "static_cdn_asset": 2,
    "api_endpoint": 2,
    "authentication_endpoint": 1,
    "search_endpoint": 1,
    "image": 2,
    "static_asset": 2,
    "analytics_tracking": 1,
    "ad_endpoint": 1,
    "waf_challenge": 1,
    "payment_checkout": 1,
    "localization_currency": 1,
    "third_party_search": 1,
    "feature_flag": 1,
    "consent_management": 1,
    "rum_telemetry": 1,
    "affiliate_referral": 1,
}

# ---- Stage 4: Globalping ----
MEASUREMENT_TIMEOUT_S = 180   # safety cap so a stuck measurement can't hang the run forever
MEASUREMENT_POLL_S    = 2

GLOBALPING_TOKEN = os.environ.get("GLOBALPING_TOKEN", "")
if not GLOBALPING_TOKEN:
    print("WARNING: GLOBALPING_TOKEN environment variable is not set — measurements will fail authentication.")

TOKEN_GROUPS = [
    {
        "name": "TOKEN_GROUP_1",
        "token": GLOBALPING_TOKEN,
        "countries": ["Australia", "India", "Singapore", "Japan", "US", "UK", "Germany", "France", "Brazil", "Mexico", "Spain", "Morocco", "South Africa", "Nigeria", "Kenya", "Egypt", "UAE", "Saudi Arabia", "Thailand", "Indonesia"],
    },

]

COUNTRY_MAP = {
    "Australia": "AU", "Brazil": "BR", "Egypt": "EG", "France": "FR",
    "Germany": "DE", "India": "IN", "Indonesia": "ID", "Kenya": "KE",
    "Japan": "JP", "Mexico": "MX", "Morocco": "MA", "Nigeria": "NG",
    "Saudi Arabia": "SA", "Singapore": "SG", "Thailand": "TH",
    "South Africa": "ZA", "Spain": "ES", "UAE": "AE",
    "United Arab Emirates": "AE", "UK": "GB", "United Kingdom": "GB",
    "US": "US", "United States": "US",
}


# =========================================================================
# STAGE 1 — PLAYWRIGHT CAPTURE  (from endpoint_capture_v7.py)
# =========================================================================

def get_host(url):
    try:    return urlparse(url).netloc.lower()
    except: return ""

def get_path(url):
    try:    return urlparse(url).path.lower()
    except: return ""

def looks_like_image(url):
    u = url.lower()
    return (u.startswith("data:image")
        or get_path(u).endswith((".png",".jpg",".jpeg",".webp",".gif",".svg",".avif",".bmp"))
        or "_next/image" in u or "is/image" in u)

def looks_like_static(url):
    return get_path(url).endswith((".js",".css",".woff",".woff2",".ttf",".otf",".ico",".map"))

def looks_like_analytics(url):
    u = url.lower()
    return any(k in u for k in [
        "google-analytics","googletagmanager","doubleclick","facebook.com/tr",
        "connect.facebook.net","analytics","gtag/js","pixel","telemetry",
        "/collect?","segment.io","mixpanel","clevertap","moengage",
        "appsflyer","branch.io","clarity.ms","hotjar",
    ])

WAF_CHALLENGE_KEYWORDS = [
    "cdn-cgi/challenge-platform", "__cf_chl", "turnstile",
    "challenges.cloudflare.com", "_incapsula_resource", "distil_r_",
    "ddos-guard", "geo.captcha-delivery.com", "recaptcha/api",
    "recaptcha/enterprise", "hcaptcha.com", "imperva", "datadome",
    "akamai-waf", "perimeterx",
]

def looks_like_waf_challenge(url):
    return any(k in url.lower() for k in WAF_CHALLENGE_KEYWORDS)

PAYMENT_CHECKOUT_KEYWORDS = [
    "/checkout", "/payment", "/billing", "/order/confirm",
    "/cart/checkout", "create-payment-intent", "/charges", "/transactions",
    "js.stripe.com", "checkout.stripe.com", "paypal.com", "paypalobjects.com",
    "adyen.com", "checkout.com", "braintreegateway.com", "klarna.com",
    "squareup.com", "razorpay.com", "payu.", "worldpay.com",
]

def looks_like_payment(url):
    return any(k in url.lower() for k in PAYMENT_CHECKOUT_KEYWORDS)

LOCALIZATION_CURRENCY_KEYWORDS = [
    "/locale", "/lang", "/language", "/i18n", "/currency",
    "setlang", "changelocale", "/geo/currency", "currency-converter",
]

def looks_like_localization(url):
    return any(k in url.lower() for k in LOCALIZATION_CURRENCY_KEYWORDS)

THIRD_PARTY_SEARCH_HOSTS = [
    "algolia.net", "algolianet.com", "elastic-app-search", "swiftype.com",
    "coveo.com", "bloomreach.com", "klevu.com", "searchspring.io", "constructor.io",
]
THIRD_PARTY_SEARCH_PATH_KEYWORDS = ["/query", "/search", "/suggest"]

def looks_like_third_party_search(url):
    u = url.lower()
    if not any(h in u for h in THIRD_PARTY_SEARCH_HOSTS):
        return False
    return any(k in u for k in THIRD_PARTY_SEARCH_PATH_KEYWORDS)

FEATURE_FLAG_KEYWORDS = [
    "/flags/", "/experiments", "/feature-flags", "gasv3", "/ab-test",
    "/split/", "/split?", "/split-test", "launchdarkly", "optimizely", "unleash-",
    "statsig.com", "split.io", "flagsmith.com",
]

def looks_like_feature_flag(url):
    return any(k in url.lower() for k in FEATURE_FLAG_KEYWORDS)

CONSENT_MANAGEMENT_KEYWORDS = [
    "onetrust", "cookiebot", "trustarc", "cmp/",
    "cookie-integrator", "cdn.cookielaw.org", "quantcast.com/choice",
    "didomi.io", "cookiepro.com",
]

def looks_like_consent_management(url):
    return any(k in url.lower() for k in CONSENT_MANAGEMENT_KEYWORDS)

RUM_TELEMETRY_HOSTS = [
    "newrelic.com", "datadoghq.com", "sentry.io", "bugsnag.com",
    "raygun.io", "speedcurve.com", "dynatrace.com",
]

def looks_like_rum_telemetry(url):
    return any(h in url.lower() for h in RUM_TELEMETRY_HOSTS)

AFFILIATE_REFERRAL_KEYWORDS = [
    "/click/", "/click?", "/affiliate", "/referral", "clickid", "affid",
    "utm_affiliate", "sjv.io", "cj.com", "shareasale.com", "impact.com",
    "partnerize.com", "awin1.com", "linksynergy.com",
]

def looks_like_affiliate_referral(url):
    return any(k in url.lower() for k in AFFILIATE_REFERRAL_KEYWORDS)

def looks_like_ad(url):
    u = url.lower()
    return any(k in u for k in ["doubleclick","adsystem","adserver","/ads/","googlesyndication","advertising"])

AUTH_SUBDOMAINS = [
    "login.", "ibank.", "ib.", "secure.", "netbank.", "online-banking.",
    "internetbanking", "netbanking", "mybank.", "ebanking.", "id.",
    "identity.", "accounts.", "sso.", "auth.", "idp.", "signin.",
]

def looks_like_auth(url):
    u = url.lower()
    if looks_like_analytics(u): return False
    try:
        host = urlparse(u).netloc
        if any(host.startswith(s) or s in host for s in AUTH_SUBDOMAINS):
            return True
    except Exception:
        pass
    return any(k in u for k in [
        "/login","/signin","/sign-in","/auth","/oauth","/token",
        "/session","/authenticate","/sso","/verify-otp","/otp",
        "/password","/credential","/realms/","/connect/token",
        "login?","signin?","oauth?","token?","openid-connect","saml",
        "/api/login","/api/auth","/api/signin","/api/session",
        "/api/v1/login","/api/v2/login","/user/login","/account/login",
        "/internetbanking","/netbanking","/ebanking",
    ])

def looks_like_search(url, post_body=None):
    u = url.lower()
    if looks_like_image(u) or looks_like_static(u) or looks_like_analytics(u):
        return False
    if any(k in u for k in [
        "/search","search?","search=","/query","query=","/find","find?",
        "autocomplete","autosuggest","suggest","opensearch",
        "/_search","/solr/","algolia","typeahead",
        "/api/search","/api/suggest","/api/autocomplete",
        "/sitecore/api/graph","/graphql","/api/graph","/api/graphql",
        "graph/snb","graph/query",
        "searchkeywords","search_keywords","search-keywords",
    ]):
        return True
    if post_body:
        try:
            body = post_body if not isinstance(post_body, str) else json.loads(post_body)
            if isinstance(body, list) and body:
                body = body[0]
            if isinstance(body, dict):
                op = str(body.get("operationName", "")).lower()
                variables = body.get("variables", {})
                if isinstance(variables, dict):
                    if (op == "search"
                            or "keyword" in variables
                            or "query" in variables
                            or "searchTerm" in variables):
                        return True
        except Exception:
            pass
    return False

def is_probable_cdn_host(host):
    h = host.lower()
    return any(k in h for k in [
        "cdn","static","assets","asset","img","image","images","media","content",
        "cloudfront","akamai","akamaized","fastly","cloudflare","edgekey",
        "edgesuite","azureedge","gstatic","googleusercontent","s3.amazonaws",
    ])

def classify(url, host, source_domain, post_body=None):
    if url.rstrip("/") == source_domain.rstrip("/"): return "main_page"
    if looks_like_rum_telemetry(url): return "rum_telemetry"
    if looks_like_analytics(url):   return "analytics_tracking"
    if looks_like_ad(url):          return "ad_endpoint"
    if looks_like_waf_challenge(url): return "waf_challenge"
    if looks_like_payment(url):     return "payment_checkout"
    if looks_like_consent_management(url): return "consent_management"
    if looks_like_feature_flag(url): return "feature_flag"
    if looks_like_affiliate_referral(url): return "affiliate_referral"
    if looks_like_localization(url): return "localization_currency"
    if looks_like_third_party_search(url): return "third_party_search"
    if looks_like_auth(url):        return "authentication_endpoint"
    if looks_like_search(url, post_body): return "search_endpoint"
    if looks_like_image(url):       return "image"
    if looks_like_static(url):
        return "static_cdn_asset" if is_probable_cdn_host(host) else "static_asset"
    if is_probable_cdn_host(host):  return "static_cdn_asset"
    return "api_endpoint"

CONSENT_SELECTORS = [
    "button:has-text('Accept all')", "button:has-text('Accept All')",
    "button:has-text('Accept cookies')", "button:has-text('Accept Cookies')",
    "button:has-text('Accept')", "button:has-text('I agree')",
    "button:has-text('I Agree')", "button:has-text('Agree')",
    "button:has-text('Allow all')", "button:has-text('Allow All')",
    "button:has-text('OK')", "button:has-text('Ok')",
    "button:has-text('Got it')", "button:has-text('Continue')",
    "button:has-text('Accepter')", "button:has-text('Tout accepter')",
    "button:has-text('Akzeptieren')", "button:has-text('Zustimmen')",
    "button:has-text('Aceptar')", "button:has-text('Aceptar todo')",
    "#onetrust-accept-btn-handler",
    "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
    "#cookie-accept", "#acceptCookies", ".cc-accept", ".cc-btn.cc-allow",
    "[data-testid*='accept' i]", "[aria-label*='accept cookies' i]",
    "[id*='consent' i] button", "[class*='consent' i] button",
    "[id*='cookie-banner' i] button", "[class*='cookie-banner' i] button",
    "[id*='cookie-notice' i] button", "[class*='cookie-notice' i] button",
]

LOGIN_SELECTORS = [
    "button:has-text('Login')", "button:has-text('Log in')",
    "button:has-text('Log In')", "button:has-text('Sign in')",
    "button:has-text('Sign In')", "button:has-text('Connexion')",
    "button:has-text('Iniciar sesión')", "button:has-text('Acceder')",
    "button:has-text('Anmelden')", "button:has-text('Inloggen')",
    "button:has-text('Entrar')", "button:has-text('登录')",
    "button:has-text('ログイン')", "button:has-text('로그인')",
    "button:has-text('تسجيل الدخول')", "button:has-text('دخول')",
    "a:has-text('Login')", "a:has-text('Log in')", "a:has-text('Log In')",
    "a:has-text('Sign in')", "a:has-text('Sign In')",
    "a:has-text('Hello, sign in')", "a:has-text('Connexion')",
    "a:has-text('Iniciar sesión')", "a:has-text('Anmelden')",
    "a:has-text('تسجيل الدخول')",
    "[data-testid*='login' i]", "[data-testid*='signin' i]",
    "[data-nav-ref*='nav_signin']",
    "[href='/login']", "[href='/signin']", "[href='/sign-in']",
    "[href*='/login']", "[href*='/signin']", "[href*='/auth/login']",
    "[href*='connexion']", "[href*='iniciar-sesion']",
    "[aria-label*='login' i]", "[aria-label*='sign in' i]",
    "[class*='login-btn' i]", "[class*='signin-btn' i]",
    "[class*='btn-login' i]", "[id*='login-btn' i]",
]

LANGUAGE_SELECTORS = [
    "button:has-text('English')", "a:has-text('English')",
    "[data-testid*='language' i] button:has-text('English')",
    "[class*='language' i] a:has-text('English')",
    "[class*='language' i] button:has-text('English')",
    "button:has-text('Continue')", "button:has-text('Proceed')",
    "button:has-text('Skip')", "a:has-text('Skip')",
    "[data-testid*='country-select' i] button",
    "[class*='country-selector' i] button",
    "[class*='language-selector' i] button",
    "[id*='country-modal' i] button",
    "[id*='language-modal' i] button",
]

AGE_GATE_SELECTORS = [
    "button:has-text('I am 18')", "button:has-text('18+')",
    "button:has-text('Yes, I am of legal age')",
    "button:has-text('I am of legal age')",
    "button:has-text('Enter')", "button:has-text('Enter Site')",
    "button:has-text('Yes')", "button:has-text('Yes, I am')",
    "[class*='age-gate' i] button", "[id*='age-gate' i] button",
    "[class*='age-verify' i] button", "[id*='age-verify' i] button",
    "[data-testid*='age' i] button",
]

APP_BANNER_SELECTORS = [
    "button:has-text('Not now')", "button:has-text('No thanks')",
    "button:has-text('Maybe later')", "button:has-text('Continue to website')",
    "button:has-text('Continue in browser')", "button:has-text('Use browser')",
    "a:has-text('Not now')", "a:has-text('Continue in browser')",
    "[class*='app-banner' i] [class*='close' i]",
    "[class*='smartbanner' i] [class*='close' i]",
    "[id*='app-banner' i] [class*='close' i]",
    ".smartbanner-close", "#smartbanner .sb-close",
    "[data-testid*='app-banner' i] button",
]

MODAL_EMAIL_SELECTORS = [
    "input[type='email']", "input[type='tel']",
    "input[name='email']", "input[name='phone']",
    "input[name='username']", "input[name='identifier']",
    "input[placeholder*='email' i]", "input[placeholder*='phone' i]",
    "input[placeholder*='mobile' i]", "input[placeholder*='username' i]",
    "input[placeholder*='Enter your email' i]",
    "[data-testid*='email' i] input", "[data-testid*='phone' i] input",
]

MODAL_SUBMIT_SELECTORS = [
    "button:has-text('Continue')", "button:has-text('Next')",
    "button:has-text('Sign in')", "button:has-text('Log in')",
    "button[type='submit']",
    "[data-testid*='continue' i]", "[data-testid*='submit' i]",
]

SEARCH_INPUT_SELECTORS = [
    "input[type='search']",
    "input[name='q']", "input[name='s']", "input[name='query']",
    "input[name='keyword']", "input[name='keywords']",
    "input[name='search']", "input[name='field-keywords']",
    "input[name='searchTerm']", "input[name='search_query']",
    "input[placeholder*='Search' i]", "input[placeholder*='Find' i]",
    "input[placeholder*='Buscar' i]", "input[placeholder*='Suche' i]",
    "input[placeholder*='Chercher' i]", "input[placeholder*='Cerca' i]",
    "input[placeholder*='بحث']", "input[placeholder*='ابحث']",
    "input[placeholder*='البحث']", "input[dir='rtl']",
    "input[aria-label*='search' i]",
    "input[id*='search' i]:not([type='hidden'])",
    "input[class*='search' i]:not([type='hidden'])",
    "input[data-testid*='search' i]",
    "form[role='search'] input", "form[action*='search'] input",
    "[role='searchbox']", "[role='combobox'][aria-label*='search' i]",
]

SEARCH_ICON_SELECTORS = [
    "button[aria-label*='search' i]", "button[title*='search' i]",
    "[class*='search-icon' i]:not(input)",
    "[class*='searchicon' i]:not(input)",
    "[class*='icon-search' i]:not(input)",
    "[class*='search-trigger' i]", "[class*='search-toggle' i]",
    "[data-testid*='search-icon' i]", "svg[aria-label*='search' i]",
    "button:has(img[src*='search'])", "a:has(img[src*='search'])",
    "[class*='search-btn' i]", "[class*='searchbtn' i]",
    "button[class*='search']", "a[class*='search']:not(input)",
    "header button",
]

def base_origin(url):
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"

def get_domain_key(domain):
    host = urlparse(domain).netloc.lower()
    return host.replace("www.", "")

def find_visible(page, selectors):
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                return el, sel
        except Exception:
            continue
    return None, None

def wait_idle(page, ms=3000):
    try:
        page.wait_for_load_state("networkidle", timeout=ms)
    except Exception:
        pass

def apply_stealth(page):
    try:
        from playwright_stealth import stealth_sync
        stealth_sync(page)
    except ImportError:
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            window.chrome = {runtime: {}};
        """)

def find_opensearch(page, store_url_fn):
    try:
        href = page.evaluate("""
            () => {
                const el = document.querySelector('link[type="application/opensearchdescription+xml"]');
                return el ? el.href : null;
            }
        """)
        if not href: return
        store_url_fn(href)
        import xml.etree.ElementTree as ET
        r = requests.get(href, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            root = ET.fromstring(r.text)
            ns = {"os": "http://a9.com/-/spec/opensearch/1.1/"}
            for u in (root.findall("os:Url", ns) + root.findall("Url")):
                tmpl = u.get("template", "")
                if tmpl:
                    concrete = (tmpl
                        .replace("{searchTerms}", SEARCH_QUERY)
                        .replace("{startPage?}", "1")
                        .replace("{count?}", "10")
                        .replace("{language?}", "en")
                        .replace("{inputEncoding?}", "UTF-8")
                        .replace("{outputEncoding?}", "UTF-8"))
                    store_url_fn(concrete)
    except Exception:
        pass

def crawl_site(item, browser):
    domain  = item["domain"]
    country = item["country"]
    bcat    = item["benchmark_category"]
    brand   = item["recommended_brand"]
    host    = urlparse(domain).netloc
    domain_key = get_domain_key(domain)
    override = SITE_OVERRIDES.get(domain_key, {})

    print(f"\n{'='*60}")
    print(f"CRAWL {brand} — {host}")
    print(f"{'='*60}")

    all_captured = {}

    def store_url(url, forced_type=None, post_body=None):
        if not url: return
        url = str(url).strip()
        if not url.startswith(("http://","https://")): return
        h  = get_host(url)
        et = forced_type if forced_type else classify(url, h, domain, post_body)
        if et not in KEEP_TYPES: return
        if url not in all_captured:
            all_captured[url] = {"host": h, "endpoint_type": et}
            if DEBUG:
                print(f"    + [{et[:6]}] {url[:90]}")
        elif PRIORITY.get(et,0) > PRIORITY.get(all_captured[url]["endpoint_type"],0):
            all_captured[url]["endpoint_type"] = et

    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 720},
        locale="en-US",
        timezone_id="America/New_York",
        permissions=[],
        geolocation=None,
    )
    page = context.new_page()
    apply_stealth(page)

    def attach_listeners(pg):
        def on_request(req):
            post_body = None
            try:
                if req.method == "POST":
                    post_data = req.post_data
                    if post_data:
                        post_body = post_data
            except Exception:
                pass
            store_url(req.url, post_body=post_body)

        def on_response(res):
            store_url(res.url)

        pg.on("request",  on_request)
        pg.on("response", on_response)

    attach_listeners(page)
    context.on("page", lambda new_pg: (attach_listeners(new_pg),
                                        print(f"    New tab: {new_pg.url[:80]}")))

    page.add_init_script("""
        (() => {
            const _c = window.__captured = window.__captured || [];
            const oF = window.fetch;
            window.fetch = function(input, init) {
                try { _c.push(typeof input==='string'?input:(input.url||'')); } catch(e) {}
                return oF.apply(this, arguments);
            };
            const oO = XMLHttpRequest.prototype.open;
            XMLHttpRequest.prototype.open = function(m, u) {
                try { if(u) _c.push(String(u)); } catch(e) {}
                return oO.apply(this, arguments);
            };
        })();
    """)

    def harvest_js(pg):
        try:
            for u in pg.evaluate("() => window.__captured || []"):
                store_url(u)
        except Exception: pass

    try:
        # STEP 1: homepage
        print(f"\n  [1/5] Loading homepage...")
        store_url(domain, "main_page")
        page.goto(domain, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        wait_idle(page, 8000)
        time.sleep(POST_LOAD_WAIT)
        harvest_js(page)

        # STEP 2: scroll
        print(f"  [2/5] Scrolling...")
        for y in [1000, 2000, 3000, 4000, 2000, 0]:
            page.mouse.wheel(0, y)
            time.sleep(SCROLL_PAUSE)
        wait_idle(page, 3000)
        harvest_js(page)

        for u in page.eval_on_selector_all("script[src]", "els=>els.map(e=>e.src).filter(Boolean)"):
            store_url(u)
        for u in page.eval_on_selector_all("link[href]", "els=>els.map(e=>e.href).filter(Boolean)"):
            store_url(u)

        find_opensearch(page, store_url)

        # STEP 2.5: dismissals
        print(f"  [2.5] Pre-consent dismissals (language/age/app)...")

        for sel in APP_BANNER_SELECTORS:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    el.click()
                    time.sleep(0.5)
                    print(f"    App banner dismissed: {sel[:50]}")
                    break
            except Exception: continue

        lang_dismissed = False
        for sel in LANGUAGE_SELECTORS:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    el.click()
                    time.sleep(1.5)
                    wait_idle(page, 3000)
                    lang_dismissed = True
                    print(f"    Language/country dismissed: {sel[:50]}")
                    break
            except Exception: continue

        if not lang_dismissed:
            try:
                result = page.evaluate("""
                    () => {
                        const kw = ['english','continue','proceed','skip'];
                        const containers = document.querySelectorAll(
                            '[id*="country"],[id*="language"],[id*="region"],' +
                            '[class*="country"],[class*="language"],[class*="region"],' +
                            '[role="dialog"]'
                        );
                        for (const c of containers) {
                            const r = c.getBoundingClientRect();
                            if (!r.width || !r.height) continue;
                            for (const btn of c.querySelectorAll('button,a,[role="button"]')) {
                                const t = btn.textContent.trim().toLowerCase();
                                if (kw.some(k => t.includes(k)) && t.length < 30) {
                                    btn.click();
                                    return btn.textContent.trim();
                                }
                            }
                        }
                        return null;
                    }
                """)
                if result:
                    time.sleep(1.5)
                    wait_idle(page, 3000)
                    print(f"    JS language dismissed: {result!r}")
            except Exception: pass

        for sel in AGE_GATE_SELECTORS:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    el.click()
                    time.sleep(1.0)
                    wait_idle(page, 3000)
                    print(f"    Age gate dismissed: {sel[:50]}")
                    break
            except Exception: continue

        # STEP 3: consent
        print(f"  [3/5] Consent dismissal...")
        dismissed = False
        for sel in CONSENT_SELECTORS:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    el.click()
                    time.sleep(CONSENT_WAIT)
                    dismissed = True
                    print(f"    Dismissed: {sel[:50]}")
                    break
            except Exception: continue

        if not dismissed:
            try:
                result = page.evaluate("""
                    () => {
                        const kw=['accept','agree','allow','got it','ok','continue',
                                  'accepter','akzeptieren','aceptar','zustimmen'];
                        for(const btn of document.querySelectorAll('button,[role="button"]')){
                            const r=btn.getBoundingClientRect();
                            if(!r.width||!r.height) continue;
                            const t=btn.textContent.trim().toLowerCase();
                            if(kw.some(k=>t.includes(k))&&t.length<40){
                                const p=btn.closest('[id*="cookie"],[id*="consent"],[id*="gdpr"],[class*="cookie"],[class*="consent"],[role="dialog"]');
                                if(p){btn.click();return btn.textContent.trim();}
                            }
                        }
                        return null;
                    }
                """)
                if result:
                    time.sleep(CONSENT_WAIT)
                    dismissed = True
                    print(f"    JS dismissed: {result!r}")
            except Exception: pass

        if dismissed:
            wait_idle(page, 3000)
            harvest_js(page)

        # STEP 4: login
        print(f"  [4/5] Login interaction...")
        auth_before = sum(1 for v in all_captured.values() if v["endpoint_type"]=="authentication_endpoint")

        login_url_override = override.get("login_url")
        if login_url_override and page.url.rstrip("/") != login_url_override.rstrip("/"):
            print(f"    Navigating to login override: {login_url_override}")
            try:
                page.goto(login_url_override, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
                wait_idle(page, 8000)
                time.sleep(3)
                harvest_js(page)
                store_url(page.url, "authentication_endpoint")
            except Exception as e:
                print(f"    Login URL nav failed: {e}")

        login_el, login_sel = find_visible(page, LOGIN_SELECTORS)

        if not login_el:
            for text in [
                "Login","Log in","Sign in","Sign In","Connexion",
                "Iniciar sesión","Anmelden","Hello, sign in",
                "تسجيل الدخول","دخول",
            ]:
                try:
                    el = page.evaluate_handle(f"""
                        () => {{
                            for(const tag of ['button','a','span','div','li'])
                                for(const el of document.querySelectorAll(tag)){{
                                    const r=el.getBoundingClientRect();
                                    if(!r.width||!r.height) continue;
                                    if(el.textContent.trim() === '{text}') return el;
                                }}
                            return null;
                        }}
                    """)
                    obj = el.as_element() if el else None
                    if obj:
                        login_el  = obj
                        login_sel = f"js:'{text}'"
                        break
                except Exception: continue

        if not login_el:
            for text in ["Internet Banking", "Online Banking", "الخدمات المصرفية"]:
                try:
                    el = page.evaluate_handle(f"""
                        () => {{
                            const scopes = document.querySelectorAll('header,nav,[class*="header"],[class*="nav-top"],[class*="topbar"]');
                            for(const scope of scopes)
                                for(const tag of ['a','button'])
                                    for(const el of scope.querySelectorAll(tag)){{
                                        const r=el.getBoundingClientRect();
                                        if(!r.width||!r.height) continue;
                                        const t=el.textContent.trim();
                                        if(t==='{text}' || t.startsWith('{text}')) return el;
                                    }}
                            return null;
                        }}
                    """)
                    obj = el.as_element() if el else None
                    if obj:
                        login_el  = obj
                        login_sel = f"js:header:'{text}'"
                        break
                except Exception: continue

        if login_el:
            try:
                clicked = False
                try:
                    page.evaluate("el => { el.scrollIntoView({block:'center'}); el.click(); }", login_el)
                    clicked = True
                except Exception:
                    pass
                if not clicked:
                    try:
                        login_el.click(force=True)
                        clicked = True
                    except Exception:
                        pass
                if not clicked:
                    raise Exception("All click methods failed")
                print(f"    Clicked: {login_sel}")
                wait_idle(page, LOGIN_WAIT * 1000)
                time.sleep(2)
                harvest_js(page)

                current_url = page.url
                if current_url and current_url != domain:
                    store_url(current_url, "authentication_endpoint")
                    print(f"    Redirected to: {current_url[:80]}")

                modal_handled = False
                for modal_sel in MODAL_EMAIL_SELECTORS:
                    try:
                        modal_input = page.query_selector(modal_sel)
                        if modal_input and modal_input.is_visible():
                            modal_input.click()
                            time.sleep(0.3)
                            modal_input.type("test@example.com", delay=80)
                            wait_idle(page, 2000)
                            time.sleep(1)
                            harvest_js(page)
                            print(f"    Modal email typed: {modal_sel[:50]}")

                            for submit_sel in MODAL_SUBMIT_SELECTORS:
                                try:
                                    sub = page.query_selector(submit_sel)
                                    if sub and sub.is_visible():
                                        sub.click()
                                        wait_idle(page, 3000)
                                        time.sleep(1.5)
                                        harvest_js(page)
                                        store_url(page.url, "authentication_endpoint")
                                        print(f"    Modal submitted: {submit_sel[:50]}")
                                        modal_handled = True
                                        break
                                except Exception: continue

                            if not modal_handled:
                                modal_handled = True
                            break
                    except Exception: continue

                for pg in context.pages:
                    try:
                        pg_url = pg.url
                        if pg_url:
                            store_url(pg_url)
                            if looks_like_auth(pg_url):
                                store_url(pg_url, "authentication_endpoint")
                        harvest_js(pg)
                        for u in pg.eval_on_selector_all("form[action]", "els=>els.map(e=>e.action).filter(Boolean)"):
                            store_url(u)
                    except Exception: pass

                auth_after = sum(1 for v in all_captured.values() if v["endpoint_type"]=="authentication_endpoint")
                print(f"    Auth endpoints captured: {auth_after - auth_before}")
            except Exception as e:
                print(f"    Login failed: {e}")
        else:
            print(f"    No login button found")

        # STEP 5: search
        print(f"  [5/5] Search interaction...")

        search_url_override = override.get("search_url")
        if search_url_override:
            print(f"    Navigating to search override: {search_url_override}")
            try:
                page.goto(search_url_override, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
                wait_idle(page, 8000)
                time.sleep(3)
                harvest_js(page)
            except Exception as e:
                print(f"    Search URL nav failed: {e}")
        else:
            try:
                page.goto(domain, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
                wait_idle(page, 5000)
                time.sleep(2)
                harvest_js(page)
            except Exception:
                pass

        search_before = sum(1 for v in all_captured.values() if v["endpoint_type"]=="search_endpoint")

        search_el, search_sel = find_visible(page, SEARCH_INPUT_SELECTORS)

        if not search_el:
            print(f"    No visible input yet — trying search icon click...")
            for icon_sel in SEARCH_ICON_SELECTORS:
                try:
                    icon = page.query_selector(icon_sel)
                    if icon and icon.is_visible():
                        icon.click()
                        time.sleep(1.5)
                        wait_idle(page, 2000)
                        search_el, search_sel = find_visible(page, SEARCH_INPUT_SELECTORS)
                        if search_el:
                            search_sel = f"icon→{search_sel}"
                            print(f"    Search icon clicked: {icon_sel[:50]}")
                            break
                except Exception: continue

        if not search_el:
            for frame in page.frames:
                if frame == page.main_frame: continue
                for sel in SEARCH_INPUT_SELECTORS:
                    try:
                        el = frame.query_selector(sel)
                        if el and el.is_visible():
                            search_el  = el
                            search_sel = f"iframe:{sel}"
                            break
                    except Exception: continue
                if search_el: break

        if not search_el:
            try:
                el = page.evaluate_handle("""
                    () => {
                        for(const inp of document.querySelectorAll(
                            'input:not([type=hidden]):not([type=password]):not([type=email]):not([type=checkbox]):not([type=radio])'
                        )){
                            const r=inp.getBoundingClientRect();
                            if(r.width<50||r.height===0) continue;
                            const ph=(inp.placeholder||'').toLowerCase();
                            const nm=(inp.name||'').toLowerCase();
                            const id=(inp.id||'').toLowerCase();
                            const cl=(inp.className||'').toLowerCase();
                            if(ph.includes('search')||nm.includes('search')||
                               id.includes('search')||cl.includes('search')||
                               ph.includes('find')||nm==='q'||r.width>250)
                                return inp;
                        }
                        return null;
                    }
                """)
                obj = el.as_element() if el else None
                if obj:
                    search_el  = obj
                    search_sel = "js:heuristic"
            except Exception: pass

        if search_el:
            try:
                search_el.scroll_into_view_if_needed()
                search_el.click()
                time.sleep(0.4)
                search_el.type(SEARCH_QUERY, delay=120)
                wait_idle(page, SEARCH_WAIT * 1000)
                time.sleep(1)
                harvest_js(page)

                search_el.press("Enter")
                wait_idle(page, SEARCH_ENTER_WAIT * 1000)
                time.sleep(1)
                store_url(page.url)
                harvest_js(page)

                search_after = sum(1 for v in all_captured.values() if v["endpoint_type"]=="search_endpoint")
                print(f"    Search ({search_sel}) -> {search_after - search_before} endpoint(s)")
            except Exception as e:
                print(f"    Search interaction failed: {e}")
        else:
            print(f"    No search input found")

    except PWTimeout:
        print(f"  Timeout: {host}")
    except Exception as e:
        print(f"  Error: {e}")
    finally:
        try: page.close()
        except: pass
        try: context.close()
        except: pass

    rows = []
    for url, data in all_captured.items():
        rows.append({
            "country":               country,
            "benchmark_category":    bcat,
            "recommended_brand_app": brand,
            "source_domain":         domain,
            "endpoint_type":         data["endpoint_type"],
            "host":                  data["host"],
            "url":                   url,
        })

    by_type = {}
    for r in rows:
        by_type.setdefault(r["endpoint_type"], 0)
        by_type[r["endpoint_type"]] += 1
    print(f"\n  DONE {host} — {len(rows)} total")
    for t, n in sorted(by_type.items()):
        print(f"     {t}: {n}")

    return rows


COLUMN_ALIASES = {
    "website_url":           ["website_url", "GlobalPing Endpoint Target", "url", "domain", "site_url", "website"],
    "country":               ["country", "Country"],
    "benchmark_category":    ["benchmark_category", "Benchmark Category", "benchmark", "category"],
    "recommended_brand_app": ["recommended_brand_app", "Recommended Brand / App", "recommended_brand", "brand", "app"],
}

def find_col(df, aliases):
    cols_lower = {c.lower(): c for c in df.columns}
    for alias in aliases:
        if alias.lower() in cols_lower:
            return cols_lower[alias.lower()]
    return None

def load_input_rows(input_csv):
    for enc in ["utf-8", "utf-8-sig", "cp1252", "latin-1"]:
        try:
            df_input = pd.read_csv(input_csv, encoding=enc)
            print(f"CSV loaded ({enc})")
            break
        except Exception:
            continue
    else:
        raise ValueError(f"Could not read CSV: {input_csv}")

    df_input.columns = df_input.columns.str.strip()
    resolved = {k: find_col(df_input, v) for k, v in COLUMN_ALIASES.items()}
    if not resolved["website_url"]:
        raise ValueError(f"Cannot find URL column. Columns: {list(df_input.columns)}")

    def get_val(row, key):
        col = resolved.get(key)
        if col and col in row.index:
            v = str(row[col]).strip()
            return "" if v.lower() == "nan" else v
        return ""

    input_rows, seen = [], set()
    for _, row in df_input.iterrows():
        target = get_val(row, "website_url")
        if not target: continue
        if not target.startswith("http"): target = "https://" + target
        if target not in seen:
            seen.add(target)
            input_rows.append({
                "domain":             target,
                "country":            get_val(row, "country"),
                "benchmark_category": get_val(row, "benchmark_category"),
                "recommended_brand":  get_val(row, "recommended_brand_app"),
            })
    print(f"Unique domains : {len(input_rows)}\n")
    return input_rows


ENDPOINTS_CSV_REQUIRED_COLS = ["country", "source_domain", "url"]

def load_endpoints_csv(path):
    """Load an already-classified endpoints CSV (the 'upload your own
    endpoints' path) — one row per URL to measure, with the caller's own
    endpoint_type labels. No discovery, no reclassification: this is handed
    straight to stage4_measure.
    """
    for enc in ["utf-8", "utf-8-sig", "cp1252", "latin-1"]:
        try:
            df = pd.read_csv(path, encoding=enc)
            break
        except Exception:
            continue
    else:
        raise ValueError(f"Could not read CSV: {path}")

    df.columns = df.columns.str.strip()
    missing = [c for c in ENDPOINTS_CSV_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Uploaded endpoints CSV is missing required column(s) {missing}. "
            f"Found: {list(df.columns)}"
        )

    for col in ["country", "benchmark_category", "recommended_brand_app",
                "source_domain", "endpoint_type", "host", "url"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
        else:
            df[col] = ""

    print(f"Endpoints CSV loaded: {len(df)} rows\n")
    return df


def stage1_crawl_all(input_rows):
    all_rows = []
    total = len(input_rows)
    print(f"STAGE 1 — Starting crawl — {total} sites  |  HEADLESS={HEADLESS}\n")
    t0 = time.time()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--start-maximized",
            ],
        )
        for i, item in enumerate(input_rows, 1):
            print(f"\n[{i}/{total}]")
            try:
                rows = crawl_site(item, browser)
            except Exception as e:
                print(f"  Unexpected error: {e}")
                rows = []
            all_rows.extend(rows)
        browser.close()

    elapsed = time.time() - t0
    print(f"\nSTAGE 1 done in {elapsed/60:.1f} min — {len(all_rows)} rows captured\n")
    return all_rows


# =========================================================================
# STAGE 2 — RE-CLASSIFY / CORRECT  (from Doublecheck_endpoint1 2.py)
# =========================================================================

def _get_host(url):
    try:
        return urlparse(str(url)).netloc.lower()
    except Exception:
        return ""

def _get_path(url):
    try:
        return urlparse(str(url)).path.lower()
    except Exception:
        return ""

def _is_empty(x):
    return pd.isna(x) or str(x).strip() == ""

def _looks_like_data_image(url):
    return str(url).lower().startswith("data:image")

def _looks_like_image(url):
    u = str(url).lower()
    path = _get_path(u)
    return (
        _looks_like_data_image(u)
        or path.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg", ".avif", ".bmp"))
        or "/image" in u
        or "is/image" in u
        or "_next/image" in u
    )

def _looks_like_static(url):
    path = _get_path(url)
    return path.endswith((
        ".js", ".css", ".ico", ".woff", ".woff2", ".ttf", ".otf",
        ".map", ".json", ".xml", ".txt"
    ))

def _looks_like_analytics(url):
    u = str(url).lower()
    keywords = [
        "google-analytics.com", "googletagmanager.com", "google.com/measurement",
        "googleadservices.com", "doubleclick.net", "facebook.com/tr",
        "connect.facebook.net", "analytics", "gtag", "pixel", "telemetry",
        "tracking", "/collect", "collect?", "segment.io", "mixpanel",
        "clevertap", "moengage", "appsflyer", "branch.io", "clarity.ms",
        "hotjar",
    ]
    return any(k in u for k in keywords)

def _looks_like_ad(url):
    u = str(url).lower()
    keywords = [
        "doubleclick", "adsystem", "adserver", "/ads/",
        "googlesyndication", "advertising",
    ]
    return any(k in u for k in keywords)

_WAF_CHALLENGE_KEYWORDS = WAF_CHALLENGE_KEYWORDS
_PAYMENT_CHECKOUT_KEYWORDS = PAYMENT_CHECKOUT_KEYWORDS
_LOCALIZATION_CURRENCY_KEYWORDS = LOCALIZATION_CURRENCY_KEYWORDS
_THIRD_PARTY_SEARCH_HOSTS = THIRD_PARTY_SEARCH_HOSTS
_THIRD_PARTY_SEARCH_PATH_KEYWORDS = THIRD_PARTY_SEARCH_PATH_KEYWORDS
_FEATURE_FLAG_KEYWORDS = FEATURE_FLAG_KEYWORDS
_CONSENT_MANAGEMENT_KEYWORDS = CONSENT_MANAGEMENT_KEYWORDS
_RUM_TELEMETRY_HOSTS = RUM_TELEMETRY_HOSTS
_AFFILIATE_REFERRAL_KEYWORDS = AFFILIATE_REFERRAL_KEYWORDS

def _looks_like_waf_challenge(url):
    return any(k in str(url).lower() for k in _WAF_CHALLENGE_KEYWORDS)

def _looks_like_payment(url):
    return any(k in str(url).lower() for k in _PAYMENT_CHECKOUT_KEYWORDS)

def _looks_like_localization(url):
    return any(k in str(url).lower() for k in _LOCALIZATION_CURRENCY_KEYWORDS)

def _looks_like_third_party_search(url):
    u = str(url).lower()
    if not any(h in u for h in _THIRD_PARTY_SEARCH_HOSTS):
        return False
    return any(k in u for k in _THIRD_PARTY_SEARCH_PATH_KEYWORDS)

def _looks_like_feature_flag(url):
    return any(k in str(url).lower() for k in _FEATURE_FLAG_KEYWORDS)

def _looks_like_consent_management(url):
    return any(k in str(url).lower() for k in _CONSENT_MANAGEMENT_KEYWORDS)

def _looks_like_rum_telemetry(url):
    return any(h in str(url).lower() for h in _RUM_TELEMETRY_HOSTS)

def _looks_like_affiliate_referral(url):
    return any(k in str(url).lower() for k in _AFFILIATE_REFERRAL_KEYWORDS)

def _looks_like_auth(url):
    u = str(url).lower()
    if _looks_like_analytics(u):
        return False
    keywords = [
        "/login", "/signin", "/sign-in", "/auth", "/oauth",
        "/token", "/session", "/authenticate", "/sso",
        "/account/login", "/user/login", "/verify-otp", "/otp",
        "login?", "signin?", "oauth?", "token?", "session?",
    ]
    return any(k in u for k in keywords)

def _looks_like_search(url):
    u = str(url).lower()
    if _looks_like_static(u) or _looks_like_image(u) or _looks_like_analytics(u):
        return False
    keywords = [
        "/search", "search?", "search=", "query=", "/query",
        "/find", "autocomplete", "suggest", "autosuggest",
        "opensearch", "search_filters",
    ]
    return any(k in u for k in keywords)

def _is_probable_cdn_host(host):
    h = str(host).lower()
    keywords = [
        "cdn", "static", "assets", "asset", "img", "image", "images",
        "media", "content", "cloudfront", "akamai", "akamaized",
        "fastly", "cloudflare", "edgekey", "edgesuite",
        "azureedge", "gstatic", "googleusercontent", "kwcdn",
    ]
    return any(k in h for k in keywords)

def _is_main_page(url, source_domain):
    try:
        return str(url).rstrip("/") == str(source_domain).rstrip("/")
    except Exception:
        return False

def _correct_endpoint_type(row):
    url = str(row.get("url", "")).strip()
    host = str(row.get("host", "")).strip().lower()
    source_domain = str(row.get("source_domain", "")).strip()
    old_type = str(row.get("endpoint_type", "")).strip()

    if _is_empty(url):
        if _is_probable_cdn_host(host):
            return "cdn_host", "URL blank and host looks like CDN"
        return "host", "URL blank, host-only record"

    if _is_main_page(url, source_domain):
        return "main_page", "URL matches source domain"

    if _looks_like_rum_telemetry(url):
        return "rum_telemetry", "RUM/app-performance telemetry vendor host detected"

    if _looks_like_analytics(url):
        return "analytics_tracking", "Analytics/tracking URL detected"

    if _looks_like_ad(url):
        return "ad_endpoint", "Ad/advertising URL detected"

    if _looks_like_waf_challenge(url):
        return "waf_challenge", "Bot-detection/WAF challenge URL detected"

    if _looks_like_payment(url):
        return "payment_checkout", "Payment/checkout gateway URL detected"

    if _looks_like_consent_management(url):
        return "consent_management", "Cookie/privacy consent manager URL detected"

    if _looks_like_feature_flag(url):
        return "feature_flag", "Feature-flag/experimentation service URL detected"

    if _looks_like_affiliate_referral(url):
        return "affiliate_referral", "Affiliate/referral tracking URL detected"

    if _looks_like_localization(url):
        return "localization_currency", "Locale/language/currency URL detected"

    if _looks_like_third_party_search(url):
        return "third_party_search", "Third-party search-as-a-service host detected"

    if _looks_like_auth(url):
        return "authentication_endpoint", "Login/auth/session/token keyword detected"

    if _looks_like_search(url):
        return "search_endpoint", "Search/query/suggest keyword detected"

    if _looks_like_image(url):
        return "image", "Image URL detected"

    if _looks_like_static(url):
        if _is_probable_cdn_host(host or _get_host(url)):
            return "static_cdn_asset", "Static file hosted on CDN-like host"
        return "static_asset", "Static file detected"

    if old_type == "api_endpoint":
        return "api_endpoint", "Kept original API endpoint classification"

    if _is_probable_cdn_host(host or _get_host(url)):
        return "static_cdn_asset", "CDN-like host detected"

    return "api_endpoint", "Dynamic URL, non-static, non-image, non-analytics"

def stage2_correct(df):
    print("STAGE 2 — Re-classifying endpoint types...")
    df = df.copy()
    df["original_endpoint_type"] = df["endpoint_type"]
    results = df.apply(_correct_endpoint_type, axis=1, result_type="expand")
    df["corrected_endpoint_type"] = results[0]
    df["correction_reason"] = results[1]
    print(df["corrected_endpoint_type"].value_counts())
    print()
    return df


# =========================================================================
# STAGE 3 — FILTER + RECOVERY  (from filter_endpoint_with_recovery.py)
# =========================================================================

def _clean_host(host, url=""):
    h = str(host).strip().lower()
    if h:
        return h
    return _get_host(url)

def _score_url(row, etype_col="corrected_endpoint_type"):
    url = str(row["url"]).lower()
    host = _clean_host(row.get("host", ""), row.get("url", ""))
    path = _get_path(row["url"])
    etype = str(row[etype_col]).strip()

    score = 0
    bad_words = [
        "tracking", "analytics", "pixel", "beacon", "ads", "doubleclick",
        "facebook", "googletag", "gtag", "captcha", "metrics", "collect",
    ]
    if any(x in url for x in bad_words):
        score -= 100

    source_host = _get_host(row["source_domain"])
    if source_host and source_host.replace("www.", "") in host:
        score += 20

    if etype == "main_page":
        score += 100
    elif etype == "static_cdn_asset":
        if any(path.endswith(x) for x in [".js", ".css"]):
            score += 40
        if any(x in url for x in ["main", "app", "bundle", "runtime", "vendor"]):
            score += 20
        if any(path.endswith(x) for x in [".png", ".jpg", ".jpeg", ".webp", ".svg", ".gif"]):
            score += 5
    elif etype == "api_endpoint":
        if any(x in url for x in ["api", "graphql", "ajax", "service", "gateway", "v1", "v2"]):
            score += 40
        if any(x in url for x in ["search", "product", "home", "catalog", "content"]):
            score += 20
    elif etype == "image":
        if any(path.endswith(x) for x in [".png", ".jpg", ".jpeg", ".webp", ".svg", ".gif", ".avif", ".bmp"]):
            score += 40
    elif etype == "static_asset":
        if any(path.endswith(x) for x in [".js", ".css"]):
            score += 40
        if any(x in url for x in ["main", "app", "bundle", "runtime", "vendor"]):
            score += 20
    elif etype == "authentication_endpoint":
        if any(x in url for x in ["login", "signin", "sign-in", "auth", "oauth", "token", "session", "sso", "otp"]):
            score += 50
    elif etype == "search_endpoint":
        if any(x in url for x in ["search", "query", "suggest", "autocomplete", "autosuggest", "find"]):
            score += 50
    elif etype == "payment_checkout":
        if any(x in url for x in PAYMENT_CHECKOUT_KEYWORDS):
            score += 50
    elif etype == "third_party_search":
        if any(x in url for x in THIRD_PARTY_SEARCH_PATH_KEYWORDS):
            score += 40
    elif etype == "localization_currency":
        if any(x in url for x in LOCALIZATION_CURRENCY_KEYWORDS):
            score += 30
    elif etype in ("waf_challenge", "consent_management", "feature_flag",
                    "affiliate_referral", "rum_telemetry"):
        score += 10

    if len(url) < 150:
        score += 10
    elif len(url) > 300:
        score -= 20

    return score

def _recovery_score(url, missing_type):
    if missing_type == "main_page":
        return 0

    u = str(url).lower()
    bad_words = [
        "analytics", "tracking", "pixel", "doubleclick", "facebook.com/tr",
        "collect", "gtag", "googletag", "beacon", "captcha",
    ]
    if any(x in u for x in bad_words):
        return -100

    matched = False
    if missing_type == "authentication_endpoint":
        keywords = ["login", "signin", "sign-in", "auth", "oauth", "token", "session", "sso", "otp", "verify"]
        matched = any(x in u for x in keywords)
    elif missing_type == "search_endpoint":
        keywords = ["search", "query", "suggest", "autocomplete", "autosuggest", "find"]
        matched = any(x in u for x in keywords)
    elif missing_type == "api_endpoint":
        keywords = ["api", "graphql", "ajax", "service", "gateway", "/v1/", "/v2/", "/v3/"]
        matched = any(x in u for x in keywords)
    elif missing_type == "static_cdn_asset":
        keywords = [".js", ".css", "cdn", "static", "assets", "cloudfront", "akamai", "fastly", "bundle", "runtime", "vendor"]
        matched = any(x in u for x in keywords)
    elif missing_type == "image":
        keywords = [".png", ".jpg", ".jpeg", ".webp", ".svg", ".gif", ".avif", ".bmp"]
        matched = any(x in u for x in keywords)
    elif missing_type == "static_asset":
        keywords = [".js", ".css", "bundle", "runtime", "vendor", "main", "app"]
        matched = any(x in u for x in keywords)
    elif missing_type == "analytics_tracking":
        keywords = ["analytics", "gtag", "googletagmanager", "pixel", "tracking", "segment.io", "mixpanel"]
        matched = any(x in u for x in keywords)
    elif missing_type == "ad_endpoint":
        keywords = ["doubleclick", "adsystem", "adserver", "/ads/", "googlesyndication", "advertising"]
        matched = any(x in u for x in keywords)
    elif missing_type == "waf_challenge":
        matched = any(x in u for x in WAF_CHALLENGE_KEYWORDS)
    elif missing_type == "payment_checkout":
        matched = any(x in u for x in PAYMENT_CHECKOUT_KEYWORDS)
    elif missing_type == "localization_currency":
        matched = any(x in u for x in LOCALIZATION_CURRENCY_KEYWORDS)
    elif missing_type == "third_party_search":
        matched = any(h in u for h in THIRD_PARTY_SEARCH_HOSTS) and any(k in u for k in THIRD_PARTY_SEARCH_PATH_KEYWORDS)
    elif missing_type == "feature_flag":
        matched = any(x in u for x in FEATURE_FLAG_KEYWORDS)
    elif missing_type == "consent_management":
        matched = any(x in u for x in CONSENT_MANAGEMENT_KEYWORDS)
    elif missing_type == "rum_telemetry":
        matched = any(h in u for h in RUM_TELEMETRY_HOSTS)
    elif missing_type == "affiliate_referral":
        matched = any(x in u for x in AFFILIATE_REFERRAL_KEYWORDS)

    if not matched:
        return 0

    score = 100
    if len(u) < 180:
        score += 10
    elif len(u) > 350:
        score -= 20

    return score

def stage3_filter_and_recover(df_all):
    print("STAGE 3 — Filtering + recovering missing endpoint types...")
    df_all = df_all.copy()
    df_all["corrected_endpoint_type"] = df_all["corrected_endpoint_type"].fillna("").astype(str).str.strip()
    df_all["url"] = df_all["url"].fillna("").astype(str).str.strip()
    df_all["host"] = df_all["host"].fillna("").astype(str).str.strip()
    df_all["country"] = df_all["country"].fillna("").astype(str).str.strip()
    df_all["source_domain"] = df_all["source_domain"].fillna("").astype(str).str.strip()
    df_all["benchmark_category"] = df_all["benchmark_category"].fillna("").astype(str).str.strip()
    df_all["recommended_brand_app"] = df_all["recommended_brand_app"].fillna("").astype(str).str.strip()

    KEEP_TYPES_3 = list(LIMITS.keys())

    # ---- normal selection ----
    df = df_all.copy()
    df = df.dropna(subset=["corrected_endpoint_type", "url"])
    df = df[df["url"].str.strip() != ""]
    df = df[df["corrected_endpoint_type"].isin(KEEP_TYPES_3)].copy()
    df = df.drop_duplicates(subset=["country", "source_domain", "corrected_endpoint_type", "host", "url"])

    df["score"] = df.apply(_score_url, axis=1)
    df["endpoint_source"] = "PLAYWRIGHT_DISCOVERED"
    df["endpoint_confidence"] = "HIGH"

    selected_rows = []
    group_cols = [
        "country", "benchmark_category", "recommended_brand_app",
        "source_domain", "corrected_endpoint_type",
    ]

    for group_values, g in df.groupby(group_cols, dropna=False):
        corrected_endpoint_type = group_values[-1]
        limit = LIMITS[corrected_endpoint_type]
        g = g.sort_values("score", ascending=False).copy()

        if limit == 1:
            selected_rows.append(g.head(1))
            continue

        picked = []
        used_hosts = set()
        for _, row in g.iterrows():
            host = _clean_host(row["host"], row["url"])
            if host not in used_hosts:
                picked.append(row)
                used_hosts.add(host)
            if len(picked) == limit:
                break

        if len(picked) < limit:
            picked_urls = set([r["url"] for r in picked])
            for _, row in g.iterrows():
                if row["url"] not in picked_urls:
                    picked.append(row)
                    picked_urls.add(row["url"])
                if len(picked) == limit:
                    break

        selected_rows.append(pd.DataFrame(picked))

    if selected_rows:
        final_df = pd.concat(selected_rows, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=df.columns)

    # ---- find missing types ----
    required_endpoint_types = list(LIMITS.keys())
    app_cols = ["country", "benchmark_category", "recommended_brand_app", "source_domain"]
    apps_df = df_all[app_cols].drop_duplicates()

    missing_rows = []
    for _, app in apps_df.iterrows():
        country = app["country"]
        benchmark_category = app["benchmark_category"]
        recommended_brand = app["recommended_brand_app"]
        source_domain = app["source_domain"]

        g = final_df[
            (final_df["country"].astype(str) == str(country)) &
            (final_df["benchmark_category"].astype(str) == str(benchmark_category)) &
            (final_df["recommended_brand_app"].astype(str) == str(recommended_brand)) &
            (final_df["source_domain"].astype(str) == str(source_domain))
        ]
        available_types = set(g["corrected_endpoint_type"].unique()) if not g.empty else set()
        for required_type in required_endpoint_types:
            if required_type not in available_types:
                missing_rows.append({
                    "country": country,
                    "benchmark_category": benchmark_category,
                    "recommended_brand_app": recommended_brand,
                    "source_domain": source_domain,
                    "missing_endpoint_type": required_type,
                })

    missing_df = pd.DataFrame(missing_rows)

    # ---- recover from existing master URLs ----
    recovered_rows = []
    pending_rows = []

    selected_keys = set(
        zip(
            final_df["country"].astype(str),
            final_df["source_domain"].astype(str),
            final_df["corrected_endpoint_type"].astype(str),
            final_df["url"].astype(str),
        )
    ) if not final_df.empty else set()

    for _, miss in missing_df.iterrows():
        country = str(miss["country"])
        benchmark_category = str(miss["benchmark_category"])
        recommended_brand = str(miss["recommended_brand_app"])
        source_domain = str(miss["source_domain"])
        missing_type = str(miss["missing_endpoint_type"])

        candidates = df_all[
            (df_all["country"].astype(str) == country) &
            (df_all["benchmark_category"].astype(str) == benchmark_category) &
            (df_all["recommended_brand_app"].astype(str) == recommended_brand) &
            (df_all["source_domain"].astype(str) == source_domain) &
            (df_all["url"].astype(str).str.strip() != "")
        ].copy()

        if candidates.empty:
            pending_rows.append(miss.to_dict())
            continue

        candidates = candidates[~candidates.apply(
            lambda r: (str(r["country"]), str(r["source_domain"]), missing_type, str(r["url"])) in selected_keys,
            axis=1
        )].copy()

        if candidates.empty:
            pending_rows.append(miss.to_dict())
            continue

        candidates["recovery_score"] = candidates["url"].apply(lambda x: _recovery_score(x, missing_type))
        candidates = candidates[candidates["recovery_score"] > 0]
        candidates = candidates.sort_values("recovery_score", ascending=False)

        if candidates.empty:
            pending_rows.append(miss.to_dict())
            continue

        best = candidates.iloc[0].copy()
        best["original_corrected_endpoint_type"] = best.get("corrected_endpoint_type", "")
        best["corrected_endpoint_type"] = missing_type
        best["score"] = best["recovery_score"]
        best["endpoint_source"] = "RECOVERED_FROM_EXISTING_URLS"
        best["endpoint_confidence"] = "MEDIUM"
        best["recovery_reason"] = f"Recovered using keyword match for missing type: {missing_type}"
        recovered_rows.append(best)

    recovered_df = pd.DataFrame(recovered_rows)
    pending_df = pd.DataFrame(pending_rows) if pending_rows else pd.DataFrame(columns=missing_df.columns)

    # ---- combine ----
    if not recovered_df.empty:
        for col in recovered_df.columns:
            if col not in final_df.columns:
                final_df[col] = ""
        for col in final_df.columns:
            if col not in recovered_df.columns:
                recovered_df[col] = ""
        final_with_recovery_df = pd.concat([final_df, recovered_df[final_df.columns]], ignore_index=True)
    else:
        final_with_recovery_df = final_df.copy()

    final_with_recovery_df = final_with_recovery_df.drop_duplicates(
        subset=["country", "source_domain", "corrected_endpoint_type", "host", "url"],
        keep="first",
    )

    print(f"  Selected rows before recovery : {len(final_df)}")
    print(f"  Missing rows before recovery   : {len(missing_df)}")
    print(f"  Recovered rows                 : {len(recovered_df)}")
    print(f"  Still pending after recovery   : {len(pending_df)}")
    if not pending_df.empty:
        print("  Pending endpoints:")
        for _, row in pending_df.iterrows():
            print(f"    [{row['country']}] {row['recommended_brand_app']} — missing {row['missing_endpoint_type']} ({row['source_domain']})")
    print()

    return final_with_recovery_df


def filter_by_types(corrected_df, types=None):
    """No scoring, no limits, no recovery — just every discovered row matching
    the requested endpoint type(s), deduplicated. types=None/[]/["all"] (case-
    insensitive) means no filter at all: every type is returned as-is. This is
    the default discovery path; stage3_filter_and_recover() is left untouched
    for the separate, user-triggered "let AI decide" action.
    """
    df = corrected_df.copy()
    df["url"] = df["url"].fillna("").astype(str).str.strip()
    df = df[df["url"] != ""]
    if types and "all" not in [str(t).strip().lower() for t in types]:
        df = df[df["corrected_endpoint_type"].isin(types)]
    return df.drop_duplicates(
        subset=["country", "source_domain", "corrected_endpoint_type", "host", "url"]
    )


# =========================================================================
# STAGE 4 — GLOBALPING MEASUREMENT  (from gp_parallel_tokens11.ps1)
# =========================================================================

def _poll_measurement(measurement_id, token, timeout_s=MEASUREMENT_TIMEOUT_S, interval_s=MEASUREMENT_POLL_S):
    headers = {"Authorization": f"Bearer {token}"}
    deadline = time.time() + timeout_s
    while True:
        r = requests.get(f"https://api.globalping.io/v1/measurements/{measurement_id}", headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "finished":
            return data
        if time.time() > deadline:
            raise TimeoutError(f"Measurement {measurement_id} did not finish within {timeout_s}s")
        time.sleep(interval_s)

def _run_ping(target, country_code, token):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "type": "ping",
        "target": target,
        "locations": [{"country": country_code, "limit": 1}],
        "measurementOptions": {"packets": 5},
    }
    r = requests.post("https://api.globalping.io/v1/measurements", headers=headers, json=body, timeout=30)
    r.raise_for_status()
    return _poll_measurement(r.json()["id"], token)

def _run_http(target, path, protocol, port, is_default_port, country_code, token):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    measurement_options = {"protocol": protocol, "request": {"method": "GET", "path": path}}
    if not is_default_port:
        measurement_options["port"] = port
    body = {
        "type": "http",
        "target": target,
        "locations": [{"country": country_code, "limit": 1}],
        "measurementOptions": measurement_options,
    }
    r = requests.post("https://api.globalping.io/v1/measurements", headers=headers, json=body, timeout=30)
    r.raise_for_status()
    return _poll_measurement(r.json()["id"], token)

def _compute_jitter(rtts):
    if len(rtts) < 2:
        return None
    diffs = [abs(rtts[i] - rtts[i-1]) for i in range(1, len(rtts))]
    return round(sum(diffs) / len(diffs), 3)

def _build_ping_map(ping_result):
    ping_map = {}
    for p in ping_result.get("results", []):
        probe = p.get("probe", {}) or {}
        key = (probe.get("country"), probe.get("city"), probe.get("asn"), probe.get("network"))
        result = p.get("result", {}) or {}
        stats = result.get("stats", {}) or {}
        timings = result.get("timings", []) or []
        rtts = [float(t["rtt"]) for t in timings if t.get("rtt") is not None]
        ping_map[key] = {
            "Ping_ResolvedIP": result.get("resolvedAddress"),
            "Packets_Sent": stats.get("total"),
            "Packets_Recv": stats.get("rcv"),
            "Packets_Drop": stats.get("drop"),
            "PacketLoss_pct": stats.get("loss"),
            "Ping_Min_ms": stats.get("min"),
            "Ping_Avg_ms": stats.get("avg"),
            "Ping_Max_ms": stats.get("max"),
            "Jitter_ms": _compute_jitter(rtts),
        }
    return ping_map

def _merge_http_ping(http_result, ping_map, group_name, country_name, row, url, target):
    out_rows = []
    for h in http_result.get("results", []):
        probe = h.get("probe", {}) or {}
        key = (probe.get("country"), probe.get("city"), probe.get("asn"), probe.get("network"))
        result = h.get("result", {}) or {}
        t = result.get("timings", {}) or {}
        ping = ping_map.get(key, {})

        effective_latency = None
        latency_type = ""
        if ping.get("Packets_Recv") and ping["Packets_Recv"] > 0:
            effective_latency = ping.get("Ping_Avg_ms")
            latency_type = "ICMP"
        elif t.get("tcp") is not None:
            effective_latency = t.get("tcp")
            latency_type = "TCP"
        elif t.get("firstByte") is not None:
            effective_latency = t.get("firstByte")
            latency_type = "TTFB"

        status_code = result.get("statusCode")
        success = "Y" if status_code is not None and 200 <= status_code < 400 else "N"

        out_rows.append({
            "Timestamp_UTC": http_result.get("updatedAt"),
            "Token_Group": group_name,
            "Input_Country": country_name,
            "Probe_Country": probe.get("country"),
            "Probe_State": probe.get("state"),
            "Probe_City": probe.get("city"),
            "Probe_Network": probe.get("network"),
            "Probe_ASN": probe.get("asn"),
            "Source_Domain": row.get("source_domain"),
            "Endpoint_Type": row.get("endpoint_type"),
            "Host": target,
            "URL": url,
            "HTTP_StatusCode": status_code,
            "Success": success,
            "HTTP_ResolvedIP": result.get("resolvedAddress"),
            "DNS_ms": t.get("dns"),
            "TCP_ms": t.get("tcp"),
            "TLS_ms": t.get("tls"),
            "TTFB_ms": t.get("firstByte"),
            "HTTP_Total_ms": t.get("total"),
            "Download_ms": t.get("download"),
            "Ping_ResolvedIP": ping.get("Ping_ResolvedIP"),
            "Packets_Sent": ping.get("Packets_Sent"),
            "Packets_Received": ping.get("Packets_Recv"),
            "Packets_Dropped": ping.get("Packets_Drop"),
            "PacketLoss_pct": ping.get("PacketLoss_pct"),
            "Ping_Min_ms": ping.get("Ping_Min_ms"),
            "Ping_Avg_ms": ping.get("Ping_Avg_ms"),
            "Ping_Max_ms": ping.get("Ping_Max_ms"),
            "Jitter_ms": ping.get("Jitter_ms"),
            "Effective_Latency_ms": effective_latency,
            "Latency_Type": latency_type,
        })
    return out_rows

def _run_token_group(group, rows_for_group):
    token = group["token"]
    group_name = group["name"]
    results, failures = [], []

    for row in rows_for_group:
        try:
            country_name = str(row.get("country", "")).strip()
            url = str(row.get("url", "")).strip()

            if not url:
                raise ValueError("URL is blank")
            if country_name not in COUNTRY_MAP:
                raise ValueError(f"Unknown country mapping: {country_name}")

            country_code = COUNTRY_MAP[country_name]
            parsed = urlparse(url)
            target = parsed.hostname
            if not target:
                raise ValueError(f"Could not parse host from URL: {url}")

            protocol = (parsed.scheme or "https").upper()
            path = parsed.path or "/"
            if parsed.query:
                path += "?" + parsed.query
            port = parsed.port
            is_default_port = port is None

            print(f"[{group_name}] Running {url} from {country_name} ({country_code})")

            ping_result = _run_ping(target, country_code, token)
            http_result = _run_http(target, path, protocol, port, is_default_port, country_code, token)
            ping_map = _build_ping_map(ping_result)
            results.extend(_merge_http_ping(http_result, ping_map, group_name, country_name, row, url, target))

        except Exception as e:
            failures.append({
                "Timestamp_Local": datetime.now().isoformat(),
                "Token_Group": group_name,
                "Country": row.get("country"),
                "URL": row.get("url"),
                "Source_Domain": row.get("source_domain"),
                "Endpoint_Type": row.get("endpoint_type"),
                "Error_Message": str(e),
            })
            print(f"FAILED [{group_name}] {row.get('country')} | {row.get('url')} : {e}")

    return results, failures

def _group_for_country(country_name, country_to_group_by_code):
    """Resolve a group by country CODE, not raw string — so 'United Kingdom'
    and 'UK' (or 'United States'/'US', 'United Arab Emirates'/'UAE') both
    resolve to the same token group even though TOKEN_GROUPS only lists one
    spelling."""
    code = COUNTRY_MAP.get(str(country_name).strip())
    if not code:
        return None
    return country_to_group_by_code.get(code)

def stage4_measure(final_df):
    print("STAGE 4 — Running Globalping measurements...")
    df = final_df.rename(columns={"corrected_endpoint_type": "endpoint_type"}).copy()
    df["country"] = df["country"].astype(str).str.strip()
    df["url"] = df["url"].astype(str).str.strip()
    df = df[df["url"] != ""]
    df = df.drop_duplicates(subset=["country", "url"])

    country_to_group_by_code = {}
    for group in TOKEN_GROUPS:
        for c in group["countries"]:
            code = COUNTRY_MAP.get(c)
            if code:
                country_to_group_by_code[code] = group

    rows_by_group = {g["name"]: [] for g in TOKEN_GROUPS}
    unassigned = []
    for _, row in df.iterrows():
        group = _group_for_country(row["country"], country_to_group_by_code)
        if group:
            rows_by_group[group["name"]].append(row.to_dict())
        else:
            unassigned.append(row.to_dict())

    all_results, all_failures = [], []
    for r in unassigned:
        all_failures.append({
            "Timestamp_Local": datetime.now().isoformat(),
            "Token_Group": "",
            "Country": r.get("country"),
            "URL": r.get("url"),
            "Source_Domain": r.get("source_domain"),
            "Endpoint_Type": r.get("endpoint_type"),
            "Error_Message": "No token group configured for this country",
        })

    with ThreadPoolExecutor(max_workers=len(TOKEN_GROUPS)) as ex:
        futures = {
            ex.submit(_run_token_group, group, rows_by_group[group["name"]]): group["name"]
            for group in TOKEN_GROUPS
        }
        for fut in as_completed(futures):
            results, failures = fut.result()
            all_results.extend(results)
            all_failures.extend(failures)

    results_df = pd.DataFrame(all_results)
    failures_df = pd.DataFrame(all_failures)
    print(f"  Measurements OK: {len(results_df)}   Failures: {len(failures_df)}\n")
    return results_df, failures_df


# =========================================================================
# ENTRY POINTS
# =========================================================================

def build_single_site_input(url, country, benchmark_category="", brand=""):
    """Build the one-row input_rows list the pipeline expects, from the
    fields your form collects: App/source URL, Country, Benchmark category.
    `country` should be the country's display name (e.g. "United Kingdom",
    "India") — it's resolved against COUNTRY_MAP downstream, so either the
    long or short form works.
    """
    target = str(url).strip()
    if not target.startswith("http"):
        target = "https://" + target
    return [{
        "domain": target,
        "country": str(country).strip(),
        "benchmark_category": str(benchmark_category).strip(),
        "recommended_brand": str(brand).strip(),
    }]


def run_pipeline(input_rows):
    """Run all 4 stages in memory for the given input_rows and return
    (captured_df, corrected_df, final_df, results_df, failures_df).
    Writes nothing to disk — callers decide what/where to save.
    """
    captured_rows = stage1_crawl_all(input_rows)
    captured_df = pd.DataFrame(captured_rows, columns=CAPTURE_COLUMNS).drop_duplicates(
        subset=["source_domain", "url"]
    )
    corrected_df = stage2_correct(captured_df)
    final_df = stage3_filter_and_recover(corrected_df)
    results_df, failures_df = stage4_measure(final_df)
    return captured_df, corrected_df, final_df, results_df, failures_df


def run_measurement_only(endpoints_df):
    """Skip crawl/correct/filter entirely — the caller already knows the
    exact endpoints to test (e.g. an uploaded CSV of pre-chosen URLs).
    Returns (results_df, failures_df).
    """
    return stage4_measure(endpoints_df)


def _parse_cli_args():
    p = argparse.ArgumentParser(
        description="Run the endpoint-reachability pipeline once (batch CSV or single site)."
    )
    p.add_argument("--url", help="Single site URL to test — skips CSV input entirely")
    p.add_argument("--country", help="Country name for the single site, e.g. 'India' or 'United Kingdom'")
    p.add_argument("--category", default="", help="Benchmark category, e.g. 'Ecommerce'")
    p.add_argument("--brand", default="", help="Optional brand/app label")
    p.add_argument("--endpoints-csv", default=None,
                   help="CSV of already-chosen endpoints (country, source_domain, endpoint_type, "
                        "host, url, ...) — skips discovery, goes straight to measurement")
    p.add_argument("--input-csv", default=None, help=f"Override batch input CSV path (default: {INPUT_CSV})")
    p.add_argument("--output-dir", default=None, help="Directory for output CSVs (default: next to this script)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_cli_args()
    t0 = time.time()
    out_dir = Path(args.output_dir) if args.output_dir else SCRIPT_DIR

    if args.endpoints_csv:
        # --- mode 3: uploaded endpoints CSV — skip crawl/correct/filter ---
        endpoints_df = load_endpoints_csv(Path(args.endpoints_csv))
        print(f"Endpoints-upload mode: {len(endpoints_df)} rows from {args.endpoints_csv}")
        results_df, failures_df = run_measurement_only(endpoints_df)

        slug = re.sub(r"[^a-zA-Z0-9]+", "_", Path(args.endpoints_csv).stem).strip("_") or "endpoints"
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_csv = out_dir / f"result_{slug}_{stamp}.csv"
        failures_csv = out_dir / f"failures_{slug}_{stamp}.csv"

        results_csv.parent.mkdir(parents=True, exist_ok=True)
        results_df.to_csv(results_csv, index=False, encoding="utf-8")
        failures_df.to_csv(failures_csv, index=False, encoding="utf-8")

        elapsed = time.time() - t0
        print("=" * 60)
        print(f"Elapsed              : {elapsed/60:.1f} min")
        print(f"Endpoints supplied   : {len(endpoints_df)}")
        print(f"Measurements OK      : {len(results_df)}")
        print(f"Measurements failed  : {len(failures_df)}")
        print(f"RESULT_CSV={results_csv}")
        print(f"FAILURES_CSV={failures_csv}")

    elif args.url:
        # --- mode 2: single site — full pipeline, one site ---
        if not args.country:
            raise SystemExit("--country is required when using --url")
        input_rows = build_single_site_input(args.url, args.country, args.category, args.brand)
        print(f"Single-site mode: {input_rows[0]['domain']} ({input_rows[0]['country']})")

        domain_slug = re.sub(r"[^a-zA-Z0-9]+", "_", urlparse(input_rows[0]["domain"]).netloc).strip("_")
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_csv = out_dir / f"result_{domain_slug}_{stamp}.csv"
        failures_csv = out_dir / f"failures_{domain_slug}_{stamp}.csv"

        captured_df, corrected_df, final_df, results_df, failures_df = run_pipeline(input_rows)

        results_csv.parent.mkdir(parents=True, exist_ok=True)
        results_df.to_csv(results_csv, index=False, encoding="utf-8")
        failures_df.to_csv(failures_csv, index=False, encoding="utf-8")

        elapsed = time.time() - t0
        print("=" * 60)
        print(f"Elapsed              : {elapsed/60:.1f} min")
        print(f"Sites processed      : {len(input_rows)}")
        print(f"Endpoints captured   : {len(captured_df)}")
        print(f"Endpoints selected   : {len(final_df)}")
        print(f"Measurements OK      : {len(results_df)}")
        print(f"Measurements failed  : {len(failures_df)}")
        print(f"RESULT_CSV={results_csv}")
        print(f"FAILURES_CSV={failures_csv}")

    else:
        # Batch CSV discovery (mode 1) is still fully implemented below —
        # load_input_rows() / INPUT_CSV / RESULTS_CSV / FAILURES_CSV — it's
        # just not part of the current form flow. To re-enable it, replace
        # this branch with:
        #     input_rows = load_input_rows(Path(args.input_csv) if args.input_csv else INPUT_CSV)
        #     results_csv = out_dir / RESULTS_CSV.name
        #     failures_csv = out_dir / FAILURES_CSV.name
        #     captured_df, corrected_df, final_df, results_df, failures_df = run_pipeline(input_rows)
        #     ... write + print summary, same shape as the --url branch above
        raise SystemExit(
            "Nothing to run: pass --url (+ --country) for single-site discovery, "
            "or --endpoints-csv for an uploaded endpoints file. Batch CSV "
            "discovery exists in the code but is disabled for now — see the "
            "comment just above this error for how to bring it back."
        )
