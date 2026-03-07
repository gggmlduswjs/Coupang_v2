"""분석 엔진: 8개 분석 모듈 + 전략 생성"""

import re
import warnings
from collections import Counter

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

from core.config import AnalysisConfig
from core.database import CoupangDB

warnings.filterwarnings("ignore", category=RuntimeWarning)


def _safe_spearman(x, y):
    """NaN이 아닌 값만으로 Spearman 상관계수 계산"""
    mask = x.notna() & y.notna()
    if mask.sum() < 5:
        return np.nan, np.nan
    return stats.spearmanr(x[mask], y[mask])


# ──────────────────────────────────────────────
# 1. 순위 팩터 상관분석
# ──────────────────────────────────────────────

def analyze_ranking_factors(df: pd.DataFrame) -> dict:
    """리뷰수/평점/가격/할인율/배송유형 vs 자연검색순위 상관분석"""
    result = {"spearman": {}, "regression": {}, "feature_importance": {}, "vif": {}}

    organic = df[df["ad_type"] == "자연검색"].copy()
    if len(organic) < 5:
        return result

    # 숫자 변환
    organic["discount_pct"] = organic["discount_rate"].apply(
        lambda x: int(re.search(r'(\d+)', str(x)).group(1)) if pd.notna(x) and re.search(r'(\d+)', str(x)) else 0
    )
    organic["is_rocket"] = (organic["delivery_type"].isin(["로켓배송", "로켓직구", "로켓럭셔리"])).astype(int)

    factors = {
        "리뷰수": "review_count",
        "평점": "rating",
        "판매가": "sale_price",
        "할인율": "discount_pct",
        "로켓배송": "is_rocket",
    }

    # Spearman 순위상관
    for label, col in factors.items():
        corr, pval = _safe_spearman(organic["organic_rank"], organic[col])
        result["spearman"][label] = {"correlation": round(corr, 4) if not np.isnan(corr) else None,
                                     "p_value": round(pval, 4) if not np.isnan(pval) else None}

    # 다중회귀
    feature_cols = [v for v in factors.values()]
    reg_df = organic[["organic_rank"] + feature_cols].dropna()
    if len(reg_df) >= 5:
        X = reg_df[feature_cols].values
        y = reg_df["organic_rank"].values

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        lr = LinearRegression()
        lr.fit(X_scaled, y)
        result["regression"]["r_squared"] = round(float(lr.score(X_scaled, y)), 4)
        result["regression"]["coefficients"] = {
            label: round(float(coef), 4)
            for label, coef in zip(factors.keys(), lr.coef_)
        }

        # VIF (다중공선성)
        for i, label in enumerate(factors.keys()):
            others = [j for j in range(len(feature_cols)) if j != i]
            if len(others) > 0 and X_scaled.shape[0] > len(others):
                lr_vif = LinearRegression()
                lr_vif.fit(X_scaled[:, others], X_scaled[:, i])
                r2 = lr_vif.score(X_scaled[:, others], X_scaled[:, i])
                vif = 1 / (1 - r2) if r2 < 1 else float("inf")
                result["vif"][label] = round(vif, 2)

        # RandomForest Feature Importance
        rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        rf.fit(X, y)
        result["feature_importance"] = {
            label: round(float(imp), 4)
            for label, imp in zip(factors.keys(), rf.feature_importances_)
        }

    return result


# ──────────────────────────────────────────────
# 2. 키워드 매칭 분석
# ──────────────────────────────────────────────

def analyze_keyword_matching(df: pd.DataFrame, keyword: str) -> dict:
    """키워드 포함율, 위치, 연관 키워드 분석"""
    result = {"inclusion_rate": 0, "position_dist": {}, "rank_diff": {},
              "related_words": [], "mann_whitney": {}}

    if df.empty or not keyword:
        return result

    kw_lower = keyword.lower()
    df = df.copy()
    df["has_keyword"] = df["product_name"].str.lower().str.contains(kw_lower, na=False)

    # 포함율
    result["inclusion_rate"] = round(df["has_keyword"].mean() * 100, 1)

    # 위치 분포
    positions = df[df["keyword_in_name"] == 1]["keyword_position"].value_counts()
    result["position_dist"] = positions.to_dict()

    # 포함 vs 미포함 순위 차이 (자연검색만)
    organic = df[df["ad_type"] == "자연검색"].copy()
    if not organic.empty:
        with_kw = organic[organic["has_keyword"]]["organic_rank"].dropna()
        without_kw = organic[~organic["has_keyword"]]["organic_rank"].dropna()

        result["rank_diff"] = {
            "포함_평균순위": round(float(with_kw.mean()), 1) if len(with_kw) > 0 else None,
            "미포함_평균순위": round(float(without_kw.mean()), 1) if len(without_kw) > 0 else None,
            "포함_상품수": int(len(with_kw)),
            "미포함_상품수": int(len(without_kw)),
        }

        # Mann-Whitney U 검정
        if len(with_kw) >= 3 and len(without_kw) >= 3:
            stat, pval = stats.mannwhitneyu(with_kw, without_kw, alternative="less")
            result["mann_whitney"] = {
                "statistic": round(float(stat), 2),
                "p_value": round(float(pval), 4),
                "significant": pval < 0.05,
            }

    # 연관 키워드 (상품명 빈출 단어)
    words = []
    for name in df["product_name"].dropna():
        tokens = re.findall(r'[가-힣a-zA-Z0-9]+', name)
        words.extend([t for t in tokens if t.lower() != kw_lower and len(t) > 1])

    counter = Counter(words)
    result["related_words"] = [{"word": w, "count": c} for w, c in counter.most_common(20)]

    return result


# ──────────────────────────────────────────────
# 3. 광고 패턴 분석
# ──────────────────────────────────────────────

def analyze_ad_patterns(df: pd.DataFrame) -> dict:
    """광고/자연검색 비율, 삽입 위치, 특성 비교"""
    result = {"ratio": {}, "positions": [], "comparison": {}}

    if df.empty:
        return result

    ad = df[df["ad_type"] == "AD"]
    organic = df[df["ad_type"] == "자연검색"]

    result["ratio"] = {
        "총상품수": len(df),
        "광고수": len(ad),
        "자연검색수": len(organic),
        "광고비율": round(len(ad) / max(len(df), 1) * 100, 1),
    }

    # 광고 삽입 위치
    result["positions"] = ad["exposure_order"].tolist()

    # 광고 vs 자연검색 특성 비교
    for label, subset in [("광고", ad), ("자연검색", organic)]:
        if subset.empty:
            continue
        result["comparison"][label] = {
            "평균가격": round(float(subset["sale_price"].mean()), 0) if subset["sale_price"].notna().any() else None,
            "평균리뷰수": round(float(subset["review_count"].mean()), 0) if subset["review_count"].notna().any() else None,
            "평균평점": round(float(subset["rating"].mean()), 2) if subset["rating"].notna().any() else None,
            "로켓배송비율": round(
                (subset["delivery_type"].isin(["로켓배송", "로켓직구", "로켓럭셔리"]).sum() / max(len(subset), 1)) * 100, 1
            ),
        }

    return result


# ──────────────────────────────────────────────
# 4. 가격대 분석
# ──────────────────────────────────────────────

def analyze_price_distribution(df: pd.DataFrame, config: AnalysisConfig = None) -> dict:
    """가격대별 분포, 상위 10위 가격대, 최적 가격대"""
    config = config or AnalysisConfig()
    result = {"stats": {}, "bins": [], "top10_price": {}, "optimal_range": {}}

    prices = df["sale_price"].dropna()
    if prices.empty:
        return result

    # 기술통계
    result["stats"] = {
        "평균": round(float(prices.mean()), 0),
        "중앙값": round(float(prices.median()), 0),
        "표준편차": round(float(prices.std()), 0),
        "최소": int(prices.min()),
        "최대": int(prices.max()),
        "25%": round(float(prices.quantile(0.25)), 0),
        "75%": round(float(prices.quantile(0.75)), 0),
    }

    # 가격대별 분포
    bin_edges = np.linspace(prices.min(), prices.max(), config.price_bins + 1)
    labels = [f"{int(bin_edges[i]):,}~{int(bin_edges[i+1]):,}원" for i in range(len(bin_edges) - 1)]
    df_temp = df.copy()
    df_temp["price_bin"] = pd.cut(df_temp["sale_price"], bins=bin_edges, labels=labels, include_lowest=True)
    bin_counts = df_temp["price_bin"].value_counts().sort_index()
    result["bins"] = [{"range": str(idx), "count": int(cnt)} for idx, cnt in bin_counts.items()]

    # 상위 10위 가격대
    organic = df[df["ad_type"] == "자연검색"].copy()
    if not organic.empty:
        top10 = organic.nsmallest(config.top_n, "organic_rank", keep="first")
        top10_prices = top10["sale_price"].dropna()
        if not top10_prices.empty:
            result["top10_price"] = {
                "평균": round(float(top10_prices.mean()), 0),
                "최소": int(top10_prices.min()),
                "최대": int(top10_prices.max()),
                "중앙값": round(float(top10_prices.median()), 0),
            }

    # 최적 가격대 제안 (상위 10위의 25%~75%)
    if result["top10_price"]:
        top10 = organic.nsmallest(config.top_n, "organic_rank", keep="first")
        t10_prices = top10["sale_price"].dropna()
        if len(t10_prices) >= 3:
            result["optimal_range"] = {
                "하한": round(float(t10_prices.quantile(0.25)), 0),
                "상한": round(float(t10_prices.quantile(0.75)), 0),
            }

    return result


# ──────────────────────────────────────────────
# 5. 경쟁 분석
# ──────────────────────────────────────────────

def analyze_competition(df: pd.DataFrame, config: AnalysisConfig = None) -> dict:
    """상위10 vs 나머지, 경쟁 강도 지수"""
    config = config or AnalysisConfig()
    result = {"top_vs_rest": {}, "thresholds": {}, "competition_index": 0}

    organic = df[df["ad_type"] == "자연검색"].copy()
    if organic.empty:
        return result

    top = organic.nsmallest(config.top_n, "organic_rank", keep="first")
    rest = organic[~organic.index.isin(top.index)]

    for label, subset in [("상위10", top), ("나머지", rest)]:
        if subset.empty:
            continue
        result["top_vs_rest"][label] = {
            "상품수": len(subset),
            "평균리뷰수": round(float(subset["review_count"].mean()), 0) if subset["review_count"].notna().any() else 0,
            "평균평점": round(float(subset["rating"].mean()), 2) if subset["rating"].notna().any() else 0,
            "평균가격": round(float(subset["sale_price"].mean()), 0) if subset["sale_price"].notna().any() else 0,
            "로켓배송비율": round(
                subset["delivery_type"].isin(["로켓배송", "로켓직구", "로켓럭셔리"]).sum() / max(len(subset), 1) * 100, 1
            ),
        }

    # 리뷰/평점 임계값 (상위 10위 진입 기준)
    if not top.empty:
        top_reviews = top["review_count"].dropna()
        top_ratings = top["rating"].dropna()
        result["thresholds"] = {
            "최소리뷰수": int(top_reviews.min()) if len(top_reviews) > 0 else 0,
            "평균리뷰수": round(float(top_reviews.mean()), 0) if len(top_reviews) > 0 else 0,
            "최소평점": round(float(top_ratings.min()), 1) if len(top_ratings) > 0 else 0,
            "평균평점": round(float(top_ratings.mean()), 2) if len(top_ratings) > 0 else 0,
        }

    # 경쟁 강도 지수 (0~100)
    # 요소: 리뷰수 집중도, 평점 집중도, 가격 집중도
    scores = []

    # 리뷰수 편중 (상위10 리뷰 비중)
    total_reviews = organic["review_count"].sum()
    if total_reviews > 0:
        top_review_share = top["review_count"].sum() / total_reviews
        scores.append(top_review_share * 100)

    # 평점 수준 (높을수록 경쟁 치열)
    avg_rating = organic["rating"].mean()
    if not np.isnan(avg_rating):
        scores.append(avg_rating / 5.0 * 100)

    # 로켓배송 비율 (높을수록 경쟁 치열)
    rocket_rate = organic["delivery_type"].isin(["로켓배송", "로켓직구", "로켓럭셔리"]).mean()
    scores.append(rocket_rate * 100)

    result["competition_index"] = round(float(np.mean(scores)), 1) if scores else 0

    return result


# ──────────────────────────────────────────────
# 6. 판매량 추정
# ──────────────────────────────────────────────

def analyze_sales_estimation(df: pd.DataFrame, config: AnalysisConfig = None) -> dict:
    """리뷰 기반 판매량 추정 및 시장 규모 산출"""
    config = config or AnalysisConfig()
    result = {"market_size": {}, "top_products": [], "concentration": {}, "revenue_estimation": {}}

    if df.empty:
        return result

    df_calc = df.copy()
    df_calc["estimated_sales"] = (df_calc["review_count"].fillna(0) / config.review_rate).astype(int)
    df_calc["estimated_revenue"] = df_calc["estimated_sales"] * df_calc["sale_price"].fillna(0)

    total_sales = int(df_calc["estimated_sales"].sum())
    total_revenue = int(df_calc["estimated_revenue"].sum())

    result["market_size"] = {
        "총_추정_판매량": total_sales,
        "총_추정_매출": total_revenue,
        "평균_추정_판매량": round(float(df_calc["estimated_sales"].mean()), 0),
        "전환율_기준": config.review_rate,
    }

    # TOP 10 상품별 매출
    organic = df_calc[df_calc["ad_type"] == "자연검색"].copy()
    if not organic.empty:
        top10 = organic.nsmallest(config.top_n, "organic_rank", keep="first")
        for _, row in top10.iterrows():
            result["top_products"].append({
                "순위": int(row["organic_rank"]) if pd.notna(row["organic_rank"]) else 0,
                "상품명": str(row["product_name"])[:40],
                "판매가": int(row["sale_price"]) if pd.notna(row["sale_price"]) else 0,
                "리뷰수": int(row["review_count"]) if pd.notna(row["review_count"]) else 0,
                "추정_판매량": int(row["estimated_sales"]),
                "추정_매출": int(row["estimated_revenue"]),
            })

    # 판매 집중도 (상위10 점유율)
    if not organic.empty and total_sales > 0:
        top10 = organic.nsmallest(config.top_n, "organic_rank", keep="first")
        top10_sales = int(top10["estimated_sales"].sum())
        top10_revenue = int(top10["estimated_revenue"].sum())
        result["concentration"] = {
            "상위10_판매량_점유율": round(top10_sales / max(total_sales, 1) * 100, 1),
            "상위10_매출_점유율": round(top10_revenue / max(total_revenue, 1) * 100, 1),
        }

    # 매출 구간 분포
    if total_revenue > 0:
        revenue_sorted = df_calc["estimated_revenue"].sort_values(ascending=False)
        top20_pct = revenue_sorted.head(max(len(revenue_sorted) // 5, 1)).sum() / total_revenue * 100
        result["revenue_estimation"] = {
            "상위20%_매출_비중": round(float(top20_pct), 1),
            "중앙값_매출": int(df_calc["estimated_revenue"].median()),
        }

    return result


# ──────────────────────────────────────────────
# 7. 시계열 순위 변동
# ──────────────────────────────────────────────

def analyze_rank_tracking(keyword: str, db, config: AnalysisConfig = None) -> dict:
    """최신 2개 스냅샷의 순위 비교 (DB 접근 필요)"""
    config = config or AnalysisConfig()

    kw_row = db.conn.execute("SELECT id FROM keywords WHERE keyword = ?", (keyword,)).fetchone()
    if not kw_row:
        return {"tracking_available": False, "note": "키워드 없음"}

    keyword_id = kw_row["id"]
    snapshots = db.conn.execute(
        "SELECT id, collected_at FROM snapshots WHERE keyword_id = ? ORDER BY collected_at DESC LIMIT 2",
        (keyword_id,),
    ).fetchall()

    snapshot_count = len(snapshots)
    if snapshot_count < 2:
        return {"tracking_available": False, "snapshot_count": snapshot_count, "note": "스냅샷 부족 (2개 이상 필요)"}

    latest_id, prev_id = snapshots[0]["id"], snapshots[1]["id"]

    # 각 스냅샷의 product_id → organic_rank 매핑
    def _get_ranks(snap_id):
        rows = db.conn.execute(
            "SELECT product_id, organic_rank FROM products WHERE snapshot_id = ? AND ad_type = '자연검색' AND organic_rank IS NOT NULL",
            (snap_id,),
        ).fetchall()
        return {r["product_id"]: r["organic_rank"] for r in rows}

    latest_ranks = _get_ranks(latest_id)
    prev_ranks = _get_ranks(prev_id)

    common_ids = set(latest_ranks.keys()) & set(prev_ranks.keys())

    risers = []
    fallers = []
    stable = []
    changes = []

    for pid in common_ids:
        diff = prev_ranks[pid] - latest_ranks[pid]  # 양수 = 상승 (순위 숫자 감소)
        changes.append(abs(diff))
        entry = {
            "product_id": pid,
            "이전순위": prev_ranks[pid],
            "현재순위": latest_ranks[pid],
            "변동": diff,
        }
        if diff > 0:
            risers.append(entry)
        elif diff < 0:
            fallers.append(entry)
        else:
            stable.append(entry)

    risers.sort(key=lambda x: x["변동"], reverse=True)
    fallers.sort(key=lambda x: x["변동"])

    # 변동성 지수 (0~100): 평균 변동폭을 전체 상품수 대비 정규화
    max_possible = max(len(latest_ranks), 1)
    avg_change = float(np.mean(changes)) if changes else 0
    volatility = min(round(avg_change / max_possible * 100 * 10, 1), 100)  # 스케일링

    return {
        "tracking_available": True,
        "snapshot_count": snapshot_count,
        "latest_snapshot": snapshots[0]["collected_at"],
        "prev_snapshot": snapshots[1]["collected_at"],
        "common_products": len(common_ids),
        "risers": risers[:10],
        "fallers": fallers[:10],
        "stable_count": len(stable),
        "volatility_index": volatility,
        "note": f"공통 상품 {len(common_ids)}개 중 상승 {len(risers)}, 하락 {len(fallers)}, 안정 {len(stable)}",
    }


# ──────────────────────────────────────────────
# 8. 셀러 집중도
# ──────────────────────────────────────────────

def analyze_seller_concentration(df: pd.DataFrame, config: AnalysisConfig = None) -> dict:
    """셀러별 상품수, HHI 산출, 시장구조 판정"""
    config = config or AnalysisConfig()
    result = {"seller_count": 0, "multi_product_sellers": [], "hhi": 0,
              "market_structure": "", "top_sellers": []}

    if df.empty:
        return result

    # vendor_item_id에서 셀러 ID 추출 (`:` 기준 첫 부분)
    def _extract_seller(vid):
        vid_str = str(vid).strip()
        if not vid_str or vid_str in ("", "nan", "None"):
            return "unknown"
        parts = vid_str.split(":")
        return parts[0] if len(parts) > 1 else vid_str

    df_calc = df.copy()
    df_calc["seller_id"] = df_calc["vendor_item_id"].apply(_extract_seller)

    seller_counts = df_calc["seller_id"].value_counts()
    total_products = len(df_calc)
    seller_count = len(seller_counts)

    result["seller_count"] = seller_count

    # HHI 산출 (시장 점유율의 제곱 합 × 10000)
    shares = (seller_counts / max(total_products, 1) * 100).values
    hhi = round(float(np.sum(shares ** 2)), 0)
    result["hhi"] = int(hhi)

    # 시장구조 판정
    if hhi < 1500:
        result["market_structure"] = "경쟁적 시장"
    elif hhi < 2500:
        result["market_structure"] = "중간 집중 시장"
    else:
        result["market_structure"] = "고도 집중 시장"

    # 복수 상품 셀러
    multi = seller_counts[seller_counts > 1]
    result["multi_product_sellers"] = [
        {"seller_id": sid, "상품수": int(cnt)} for sid, cnt in multi.items()
    ]

    # TOP 5 셀러
    top5 = seller_counts.head(5)
    result["top_sellers"] = [
        {"seller_id": sid, "상품수": int(cnt), "점유율": round(cnt / max(total_products, 1) * 100, 1)}
        for sid, cnt in top5.items()
    ]

    return result


# ──────────────────────────────────────────────
# 9. 전략 생성
# ──────────────────────────────────────────────

def generate_strategy(ranking: dict, keyword_match: dict, ad_patterns: dict,
                      price_dist: dict, competition: dict, keyword: str,
                      sales_est: dict = None, rank_tracking: dict = None,
                      seller_conc: dict = None) -> dict:
    """분석 결과 기반 전략 제안"""
    sales_est = sales_est or {}
    rank_tracking = rank_tracking or {}
    seller_conc = seller_conc or {}
    strategy = {"pricing": {}, "keyword": {}, "review": {}, "delivery": {},
                "market_insight": {}, "rank_stability": {}, "seller_competition": {},
                "actions": []}

    # 가격 전략
    if price_dist.get("optimal_range"):
        opt = price_dist["optimal_range"]
        strategy["pricing"] = {
            "추천가격대": f"{int(opt['하한']):,}원 ~ {int(opt['상한']):,}원",
            "근거": "상위 10위 상품의 25~75% 가격 구간",
        }
    elif price_dist.get("top10_price"):
        tp = price_dist["top10_price"]
        strategy["pricing"] = {
            "추천가격대": f"{int(tp['최소']):,}원 ~ {int(tp['최대']):,}원",
            "근거": "상위 10위 상품의 가격 범위",
        }

    # 키워드 전략
    if keyword_match:
        inc = keyword_match.get("inclusion_rate", 0)
        pos = keyword_match.get("position_dist", {})
        strategy["keyword"] = {
            "키워드포함율": f"{inc}%",
            "권장_상품명_구조": "",
            "연관키워드": [w["word"] for w in keyword_match.get("related_words", [])[:5]],
        }
        # 위치 권장
        if pos:
            best_pos = max(pos, key=pos.get)
            strategy["keyword"]["권장_키워드위치"] = best_pos
            if best_pos == "앞":
                strategy["keyword"]["권장_상품명_구조"] = f"[{keyword}] + 핵심 특성 + 상세 설명"
            elif best_pos == "중":
                strategy["keyword"]["권장_상품명_구조"] = f"브랜드 + [{keyword}] + 상세 설명"
            else:
                strategy["keyword"]["권장_상품명_구조"] = f"브랜드 + 핵심 특성 + [{keyword}]"

        # Mann-Whitney 결과
        mw = keyword_match.get("mann_whitney", {})
        if mw.get("significant"):
            strategy["keyword"]["통계적유의성"] = "키워드 포함 상품의 순위가 유의미하게 높음 (p < 0.05)"

    # 리뷰 전략
    if competition.get("thresholds"):
        thr = competition["thresholds"]
        strategy["review"] = {
            "최소목표리뷰수": int(thr["최소리뷰수"]),
            "권장목표리뷰수": int(thr["평균리뷰수"]),
            "최소목표평점": thr["최소평점"],
        }

    # 배송 전략
    top_data = competition.get("top_vs_rest", {}).get("상위10", {})
    if top_data:
        rocket_pct = top_data.get("로켓배송비율", 0)
        if rocket_pct >= 70:
            strategy["delivery"] = {
                "권장": "로켓배송 필수",
                "근거": f"상위 10위의 {rocket_pct}%가 로켓배송",
            }
        elif rocket_pct >= 40:
            strategy["delivery"] = {
                "권장": "로켓배송 권장",
                "근거": f"상위 10위의 {rocket_pct}%가 로켓배송",
            }
        else:
            strategy["delivery"] = {
                "권장": "마켓플레이스도 가능",
                "근거": f"상위 10위의 로켓배송 비율이 {rocket_pct}%로 낮음",
            }

    # 시장 인사이트 (판매량 추정 기반)
    ms = sales_est.get("market_size", {})
    conc = sales_est.get("concentration", {})
    if ms:
        strategy["market_insight"] = {
            "총_추정_매출": f"{ms.get('총_추정_매출', 0):,}원",
            "총_추정_판매량": f"{ms.get('총_추정_판매량', 0):,}개",
            "상위10_매출_점유율": f"{conc.get('상위10_매출_점유율', 0)}%",
            "시장_집중도": "높음" if conc.get("상위10_매출_점유율", 0) >= 70 else "보통" if conc.get("상위10_매출_점유율", 0) >= 40 else "낮음",
        }

    # 순위 안정성 (시계열 기반)
    if rank_tracking.get("tracking_available"):
        vi = rank_tracking.get("volatility_index", 0)
        stability = "안정" if vi < 20 else "보통" if vi < 50 else "불안정"
        strategy["rank_stability"] = {
            "변동성_지수": vi,
            "안정성": stability,
            "상승_상품수": len(rank_tracking.get("risers", [])),
            "하락_상품수": len(rank_tracking.get("fallers", [])),
            "모니터링_주기": "주 1회" if stability == "안정" else "주 2~3회" if stability == "보통" else "매일",
        }
    else:
        strategy["rank_stability"] = {"note": rank_tracking.get("note", "데이터 부족")}

    # 셀러 경쟁 (HHI 기반)
    if seller_conc.get("seller_count", 0) > 0:
        hhi = seller_conc.get("hhi", 0)
        if hhi >= 2500:
            entry_diff = "매우 어려움 (소수 셀러 과점)"
        elif hhi >= 1500:
            entry_diff = "보통 (과점 경향)"
        else:
            entry_diff = "비교적 용이 (다수 셀러 경쟁)"
        strategy["seller_competition"] = {
            "셀러수": seller_conc["seller_count"],
            "HHI": hhi,
            "시장구조": seller_conc.get("market_structure", ""),
            "진입_난이도": entry_diff,
        }

    # 핵심 순위 팩터
    fi = ranking.get("feature_importance", {})
    if fi:
        sorted_factors = sorted(fi.items(), key=lambda x: x[1], reverse=True)
        top_factor = sorted_factors[0][0] if sorted_factors else ""
    else:
        top_factor = ""

    # 액션 체크리스트 (우선순위별)
    actions = []

    # 1순위: 가장 영향력 큰 팩터
    if top_factor:
        actions.append({
            "우선순위": 1,
            "항목": f"핵심 팩터 '{top_factor}' 최적화",
            "설명": f"순위에 가장 큰 영향을 미치는 요소: {top_factor} (중요도: {fi.get(top_factor, 0):.1%})",
        })

    # 2순위: 가격
    if strategy["pricing"]:
        actions.append({
            "우선순위": 2,
            "항목": "가격 설정",
            "설명": f"추천 가격대: {strategy['pricing'].get('추천가격대', '')}",
        })

    # 3순위: 상품명
    if strategy["keyword"].get("권장_상품명_구조"):
        actions.append({
            "우선순위": 3,
            "항목": "상품명 최적화",
            "설명": f"구조: {strategy['keyword']['권장_상품명_구조']}",
        })

    # 4순위: 리뷰
    if strategy["review"]:
        actions.append({
            "우선순위": 4,
            "항목": "리뷰 확보",
            "설명": f"최소 {strategy['review'].get('최소목표리뷰수', 0)}개, 목표 {strategy['review'].get('권장목표리뷰수', 0)}개",
        })

    # 5순위: 배송
    if strategy["delivery"]:
        actions.append({
            "우선순위": 5,
            "항목": "배송 전략",
            "설명": strategy["delivery"].get("권장", ""),
        })

    # 6순위: 시장 진입 기회 (판매량 기반)
    if strategy.get("market_insight") and strategy["market_insight"].get("시장_집중도"):
        concentration_level = strategy["market_insight"]["시장_집중도"]
        actions.append({
            "우선순위": 6,
            "항목": "시장 진입 기회 분석",
            "설명": f"시장 집중도: {concentration_level}, 추정 시장 규모: {strategy['market_insight'].get('총_추정_매출', '')}",
        })

    # 7순위: 셀러 경쟁 분석
    if strategy.get("seller_competition") and strategy["seller_competition"].get("진입_난이도"):
        actions.append({
            "우선순위": 7,
            "항목": "셀러 경쟁 대응",
            "설명": f"셀러 {strategy['seller_competition'].get('셀러수', 0)}개, HHI {strategy['seller_competition'].get('HHI', 0)}, 진입: {strategy['seller_competition']['진입_난이도']}",
        })

    # 8순위: 순위 모니터링
    if strategy.get("rank_stability") and strategy["rank_stability"].get("모니터링_주기"):
        actions.append({
            "우선순위": 8,
            "항목": "순위 모니터링",
            "설명": f"변동성 {strategy['rank_stability'].get('변동성_지수', 0)}/100, 권장 모니터링: {strategy['rank_stability']['모니터링_주기']}",
        })

    strategy["actions"] = actions
    strategy["competition_index"] = competition.get("competition_index", 0)

    return strategy


# ──────────────────────────────────────────────
# 통합 분석 실행
# ──────────────────────────────────────────────

def run_full_analysis(keyword: str, config: AnalysisConfig = None) -> dict:
    """키워드에 대한 전체 분석 실행"""
    config = config or AnalysisConfig()
    db = CoupangDB(config)

    df = db.get_analysis_dataframe(keyword)

    if df.empty:
        db.close()
        print(f"  '{keyword}' 데이터가 없습니다. 먼저 collect 또는 import를 실행하세요.")
        return {}

    print(f"  분석 대상: {len(df)}개 상품 (키워드: {keyword})")

    print("  [1/8] 순위 팩터 상관분석...")
    ranking = analyze_ranking_factors(df)

    print("  [2/8] 키워드 매칭 분석...")
    keyword_match = analyze_keyword_matching(df, keyword)

    print("  [3/8] 광고 패턴 분석...")
    ad_patterns = analyze_ad_patterns(df)

    print("  [4/8] 가격대 분석...")
    price_dist = analyze_price_distribution(df, config)

    print("  [5/8] 경쟁 분석...")
    competition = analyze_competition(df, config)

    print("  [6/8] 판매량 추정...")
    sales_estimation = analyze_sales_estimation(df, config)

    print("  [7/8] 시계열 순위 변동...")
    rank_tracking = analyze_rank_tracking(keyword, db, config)

    print("  [8/8] 셀러 집중도...")
    seller_concentration = analyze_seller_concentration(df, config)

    db.close()

    print("  전략 생성 중...")
    strategy = generate_strategy(
        ranking, keyword_match, ad_patterns, price_dist, competition, keyword,
        sales_est=sales_estimation, rank_tracking=rank_tracking, seller_conc=seller_concentration,
    )

    return {
        "keyword": keyword,
        "total_products": len(df),
        "dataframe": df,
        "ranking_factors": ranking,
        "keyword_matching": keyword_match,
        "ad_patterns": ad_patterns,
        "price_distribution": price_dist,
        "competition": competition,
        "sales_estimation": sales_estimation,
        "rank_tracking": rank_tracking,
        "seller_concentration": seller_concentration,
        "strategy": strategy,
    }


def print_analysis_report(analysis: dict):
    """분석 결과를 콘솔에 출력"""
    if not analysis:
        return

    SEP = "=" * 65
    print(f"\n{SEP}")
    print(f"  쿠팡 검색 알고리즘 분석 리포트")
    print(f"  키워드: {analysis['keyword']} ({analysis['total_products']}개 상품)")
    print(f"{SEP}")

    # 1. 순위 팩터
    rf = analysis["ranking_factors"]
    if rf.get("spearman"):
        print(f"\n{'─'*65}")
        print(f"  [1] 순위 팩터 상관분석 (Spearman)")
        print(f"{'─'*65}")
        for factor, vals in rf["spearman"].items():
            corr = vals.get("correlation")
            pval = vals.get("p_value")
            sig = "***" if pval and pval < 0.01 else "**" if pval and pval < 0.05 else "*" if pval and pval < 0.1 else ""
            corr_s = f"{corr:+.3f}" if corr is not None else "N/A"
            print(f"  {factor:>8}: r = {corr_s}  (p = {pval}) {sig}")

    if rf.get("feature_importance"):
        print(f"\n  RandomForest Feature Importance:")
        sorted_fi = sorted(rf["feature_importance"].items(), key=lambda x: x[1], reverse=True)
        for factor, imp in sorted_fi:
            bar = "█" * int(imp * 50)
            print(f"  {factor:>8}: {imp:.3f} {bar}")

    if rf.get("regression"):
        print(f"\n  다중회귀 R²: {rf['regression'].get('r_squared', 'N/A')}")

    # 2. 키워드 매칭
    km = analysis["keyword_matching"]
    if km.get("inclusion_rate"):
        print(f"\n{'─'*65}")
        print(f"  [2] 키워드 매칭 분석")
        print(f"{'─'*65}")
        print(f"  키워드 포함율: {km['inclusion_rate']}%")
        if km.get("position_dist"):
            print(f"  위치 분포: {km['position_dist']}")
        rd = km.get("rank_diff", {})
        if rd.get("포함_평균순위") is not None:
            print(f"  포함 평균순위: {rd['포함_평균순위']} ({rd['포함_상품수']}개)")
            print(f"  미포함 평균순위: {rd.get('미포함_평균순위', 'N/A')} ({rd.get('미포함_상품수', 0)}개)")
        mw = km.get("mann_whitney", {})
        if mw:
            sig = "유의미 ✓" if mw.get("significant") else "유의미하지 않음"
            print(f"  Mann-Whitney U: p = {mw.get('p_value')} → {sig}")
        if km.get("related_words"):
            words = [w["word"] for w in km["related_words"][:10]]
            print(f"  연관 키워드: {', '.join(words)}")

    # 3. 광고 패턴
    ap = analysis["ad_patterns"]
    if ap.get("ratio"):
        print(f"\n{'─'*65}")
        print(f"  [3] 광고 패턴 분석")
        print(f"{'─'*65}")
        r = ap["ratio"]
        print(f"  광고: {r['광고수']}개 / 자연검색: {r['자연검색수']}개 (광고비율 {r['광고비율']}%)")
        if ap.get("positions"):
            print(f"  광고 위치: {ap['positions']}")
        for label, comp in ap.get("comparison", {}).items():
            print(f"  [{label}] 평균가격: {comp.get('평균가격'):,.0f}원, "
                  f"평균리뷰: {comp.get('평균리뷰수'):,.0f}개, "
                  f"평점: {comp.get('평균평점')}, "
                  f"로켓: {comp.get('로켓배송비율')}%")

    # 4. 가격대 분석
    pd_result = analysis["price_distribution"]
    if pd_result.get("stats"):
        print(f"\n{'─'*65}")
        print(f"  [4] 가격대 분석")
        print(f"{'─'*65}")
        s = pd_result["stats"]
        print(f"  평균: {int(s['평균']):,}원 / 중앙값: {int(s['중앙값']):,}원 / 표준편차: {int(s['표준편차']):,}원")
        print(f"  범위: {s['최소']:,}원 ~ {s['최대']:,}원")
        if pd_result.get("top10_price"):
            tp = pd_result["top10_price"]
            print(f"  상위10위 가격: {int(tp['최소']):,}원 ~ {int(tp['최대']):,}원 (평균 {int(tp['평균']):,}원)")
        if pd_result.get("optimal_range"):
            opt = pd_result["optimal_range"]
            print(f"  ★ 최적 가격대: {int(opt['하한']):,}원 ~ {int(opt['상한']):,}원")

    # 5. 경쟁 분석
    comp = analysis["competition"]
    if comp.get("top_vs_rest"):
        print(f"\n{'─'*65}")
        print(f"  [5] 경쟁 분석")
        print(f"{'─'*65}")
        for label, data in comp["top_vs_rest"].items():
            print(f"  [{label}] 리뷰: {data.get('평균리뷰수'):,.0f}개, "
                  f"평점: {data.get('평균평점')}, "
                  f"가격: {int(data.get('평균가격', 0)):,}원, "
                  f"로켓: {data.get('로켓배송비율')}%")
        if comp.get("thresholds"):
            thr = comp["thresholds"]
            print(f"  진입 기준: 리뷰 {thr['최소리뷰수']}개+, 평점 {thr['최소평점']}+")
        print(f"  경쟁 강도: {comp.get('competition_index', 0)}/100")

    # 6. 판매량 추정
    se = analysis.get("sales_estimation", {})
    if se.get("market_size"):
        print(f"\n{'─'*65}")
        print(f"  [6] 판매량 추정 (리뷰 기반)")
        print(f"{'─'*65}")
        ms = se["market_size"]
        print(f"  추정 총 판매량: {ms.get('총_추정_판매량', 0):,}개")
        print(f"  추정 총 매출: {ms.get('총_추정_매출', 0):,}원")
        print(f"  전환율 기준: {ms.get('전환율_기준', 0)}")
        conc = se.get("concentration", {})
        if conc:
            print(f"  상위10 판매량 점유율: {conc.get('상위10_판매량_점유율', 0)}%")
            print(f"  상위10 매출 점유율: {conc.get('상위10_매출_점유율', 0)}%")
        if se.get("top_products"):
            print(f"  TOP 매출 상품:")
            for p in se["top_products"][:5]:
                print(f"    {p['순위']}위: {p['상품명']} ({p['추정_매출']:,}원)")

    # 7. 시계열 순위 변동
    rt = analysis.get("rank_tracking", {})
    print(f"\n{'─'*65}")
    print(f"  [7] 시계열 순위 변동")
    print(f"{'─'*65}")
    if rt.get("tracking_available"):
        print(f"  스냅샷: {rt.get('prev_snapshot', '')[:10]} → {rt.get('latest_snapshot', '')[:10]}")
        print(f"  공통 상품: {rt.get('common_products', 0)}개")
        print(f"  변동성 지수: {rt.get('volatility_index', 0)}/100")
        print(f"  {rt.get('note', '')}")
        if rt.get("risers"):
            print(f"  상승 TOP 3:")
            for r in rt["risers"][:3]:
                print(f"    {r['product_id']}: {r['이전순위']}위 → {r['현재순위']}위 (+{r['변동']})")
        if rt.get("fallers"):
            print(f"  하락 TOP 3:")
            for r in rt["fallers"][:3]:
                print(f"    {r['product_id']}: {r['이전순위']}위 → {r['현재순위']}위 ({r['변동']})")
    else:
        print(f"  {rt.get('note', '데이터 부족')}")

    # 8. 셀러 집중도
    sc = analysis.get("seller_concentration", {})
    if sc.get("seller_count", 0) > 0:
        print(f"\n{'─'*65}")
        print(f"  [8] 셀러 집중도 (HHI)")
        print(f"{'─'*65}")
        print(f"  셀러 수: {sc['seller_count']}개")
        print(f"  HHI: {sc.get('hhi', 0)} ({sc.get('market_structure', '')})")
        if sc.get("top_sellers"):
            print(f"  TOP 셀러:")
            for s in sc["top_sellers"][:5]:
                print(f"    {s['seller_id']}: {s['상품수']}개 ({s['점유율']}%)")

    # 전략
    strat = analysis["strategy"]
    if strat.get("actions"):
        print(f"\n{SEP}")
        print(f"  전략 제안")
        print(f"{SEP}")
        if strat.get("pricing"):
            print(f"  가격: {strat['pricing'].get('추천가격대', '')}")
        if strat.get("keyword", {}).get("권장_상품명_구조"):
            print(f"  상품명: {strat['keyword']['권장_상품명_구조']}")
        if strat.get("review"):
            print(f"  리뷰: 최소 {strat['review'].get('최소목표리뷰수', 0)}개, 목표 {strat['review'].get('권장목표리뷰수', 0)}개")
        if strat.get("delivery"):
            print(f"  배송: {strat['delivery'].get('권장', '')}")
        mi = strat.get("market_insight", {})
        if mi.get("총_추정_매출"):
            print(f"  시장규모: {mi['총_추정_매출']} (집중도: {mi.get('시장_집중도', '')})")
        rs = strat.get("rank_stability", {})
        if rs.get("변동성_지수") is not None:
            print(f"  순위변동: 변동성 {rs['변동성_지수']}/100 ({rs.get('안정성', '')}), 모니터링: {rs.get('모니터링_주기', '')}")
        sc_strat = strat.get("seller_competition", {})
        if sc_strat.get("셀러수"):
            print(f"  셀러경쟁: {sc_strat['셀러수']}개 셀러, HHI {sc_strat.get('HHI', 0)}, 진입: {sc_strat.get('진입_난이도', '')}")

        print(f"\n  [액션 체크리스트]")
        for action in strat["actions"]:
            print(f"  {action['우선순위']}. {action['항목']}: {action['설명']}")

    print(f"\n{SEP}")
