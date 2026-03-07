"""쿠팡 시스템 역공학 — 수집 데이터에서 내부 규칙 추출

수집된 검색 결과 데이터를 분석하여 쿠팡의:
1. SERP 구조 (광고 슬롯 규칙, 다양성 규칙)
2. ID 체계 (productId/itemId/vendorItemId = 카탈로그 구조)
3. 순위 결정 심층 (비선형 효과, 임계값, 상호작용)
4. sourceType 해독
5. 배송 유형 부스트 정량화
6. 가격 알고리즘 (최적 구간, 패널티)
7. 시간 패턴 (스냅샷 간 변동 규칙)
을 역으로 추출한다.
"""

import re
import warnings
from collections import Counter, defaultdict
from itertools import combinations

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.inspection import partial_dependence
from sklearn.preprocessing import StandardScaler

from core.config import AnalysisConfig
from core.database import CoupangDB

warnings.filterwarnings("ignore")


# ══════════════════════════════════════════════
# 1. SERP 구조 역공학
# ══════════════════════════════════════════════

def reverse_serp_structure(df: pd.DataFrame) -> dict:
    """검색 결과 페이지의 숨겨진 구조 규칙을 추출.

    - 광고 슬롯 위치 패턴 (몇 번째에 광고가 오는가)
    - 광고 간격 규칙
    - 한 페이지 내 셀러/브랜드 중복 제한 (다양성 규칙)
    - 배송유형 배치 패턴
    """
    result = {
        "ad_slot_pattern": {},
        "ad_interval_rule": {},
        "diversity_rules": {},
        "delivery_placement": {},
        "page_composition": {},
    }

    if df.empty:
        return result

    total = len(df)
    ad = df[df["ad_type"] == "AD"]
    organic = df[df["ad_type"] == "자연검색"]

    # ── 1a. 광고 슬롯 위치 패턴 ──
    ad_positions = sorted(ad["exposure_order"].tolist())
    result["ad_slot_pattern"] = {
        "ad_positions": ad_positions,
        "total_ads": len(ad_positions),
        "total_products": total,
    }

    if len(ad_positions) >= 2:
        # 광고 간격 분석
        intervals = [ad_positions[i+1] - ad_positions[i] for i in range(len(ad_positions)-1)]
        result["ad_interval_rule"] = {
            "intervals": intervals,
            "avg_interval": round(np.mean(intervals), 1),
            "min_interval": min(intervals),
            "max_interval": max(intervals),
            "stddev": round(np.std(intervals), 1),
        }

        # 광고 슬롯 고정 여부 판단
        if np.std(intervals) < 2:
            result["ad_interval_rule"]["pattern"] = f"고정 간격 (~{round(np.mean(intervals))}개마다)"
        else:
            result["ad_interval_rule"]["pattern"] = "가변 간격"

    # 페이지 구간별 광고 밀도 (1~12, 13~24, 25~36)
    page_sections = {"상단(1-12)": 0, "중단(13-24)": 0, "하단(25-36)": 0, "36+": 0}
    for pos in ad_positions:
        if pos <= 12:
            page_sections["상단(1-12)"] += 1
        elif pos <= 24:
            page_sections["중단(13-24)"] += 1
        elif pos <= 36:
            page_sections["하단(25-36)"] += 1
        else:
            page_sections["36+"] += 1
    result["ad_slot_pattern"]["section_density"] = page_sections

    # ── 1b. 다양성 규칙 (셀러/브랜드 중복 제한) ──
    def _extract_seller(vid):
        vid_str = str(vid).strip()
        if not vid_str or vid_str in ("", "nan", "None"):
            return "unknown"
        return vid_str.split(":")[0] if ":" in vid_str else vid_str

    df_work = df.copy()
    df_work["seller_id"] = df_work["vendor_item_id"].apply(_extract_seller)

    # 연속 동일 셀러 출현 확인
    consecutive_same = 0
    max_consecutive = 0
    prev_seller = None
    for _, row in df_work.iterrows():
        if row["seller_id"] == prev_seller and prev_seller != "unknown":
            consecutive_same += 1
            max_consecutive = max(max_consecutive, consecutive_same)
        else:
            consecutive_same = 0
        prev_seller = row["seller_id"]

    # 같은 셀러의 최대 출현 횟수
    seller_counts = df_work["seller_id"].value_counts()
    max_seller_appearances = int(seller_counts.max()) if not seller_counts.empty else 0

    result["diversity_rules"] = {
        "max_consecutive_same_seller": max_consecutive,
        "max_seller_appearances": max_seller_appearances,
        "unique_sellers": len(seller_counts),
        "seller_diversity_ratio": round(len(seller_counts) / max(total, 1), 3),
    }

    # 연속 셀러가 0 또는 1이면 다양성 규칙 존재
    if max_consecutive <= 1:
        result["diversity_rules"]["rule_detected"] = "연속 동일 셀러 제한 있음 (최대 2개)"
    else:
        result["diversity_rules"]["rule_detected"] = f"연속 {max_consecutive + 1}개까지 허용"

    # ── 1c. 배송유형 배치 패턴 ──
    delivery_positions = defaultdict(list)
    for _, row in df.iterrows():
        dt = row.get("delivery_type", "기타")
        delivery_positions[dt].append(row["exposure_order"])

    delivery_stats = {}
    for dt, positions in delivery_positions.items():
        delivery_stats[dt] = {
            "count": len(positions),
            "avg_position": round(np.mean(positions), 1),
            "positions_top10": len([p for p in positions if p <= 10]),
            "share_pct": round(len(positions) / max(total, 1) * 100, 1),
        }
    result["delivery_placement"] = delivery_stats

    # ── 1d. 페이지 전체 구성 비율 ──
    result["page_composition"] = {
        "ad_ratio": round(len(ad) / max(total, 1) * 100, 1),
        "organic_ratio": round(len(organic) / max(total, 1) * 100, 1),
        "rocket_ratio": round(
            df["delivery_type"].isin(["로켓배송", "로켓직구", "로켓럭셔리"]).sum() / max(total, 1) * 100, 1
        ),
        "marketplace_ratio": round(
            (df["delivery_type"] == "마켓플레이스").sum() / max(total, 1) * 100, 1
        ),
    }

    return result


# ══════════════════════════════════════════════
# 2. ID 체계 역공학 (카탈로그 구조)
# ══════════════════════════════════════════════

def reverse_id_system(df: pd.DataFrame) -> dict:
    """productId / itemId / vendorItemId 관계를 분석하여 카탈로그 구조 해독.

    쿠팡 추정 구조:
    - productId: 카탈로그 단위 (같은 상품 = 같은 productId)
    - itemId: 옵션/변형 단위 (색상, 사이즈 등)
    - vendorItemId: 셀러×상품 단위 (같은 상품이라도 셀러마다 다름)
    """
    result = {
        "id_counts": {},
        "relationships": {},
        "catalog_structure": {},
        "multi_seller_products": [],
        "multi_item_products": [],
    }

    if df.empty:
        return result

    df_work = df[df["product_id"].astype(str).str.strip() != ""].copy()

    unique_products = df_work["product_id"].nunique()
    unique_items = df_work["item_id"].nunique()
    unique_vendors = df_work["vendor_item_id"].nunique()
    total_rows = len(df_work)

    result["id_counts"] = {
        "total_rows": total_rows,
        "unique_product_ids": unique_products,
        "unique_item_ids": unique_items,
        "unique_vendor_item_ids": unique_vendors,
    }

    # 관계 비율
    result["relationships"] = {
        "rows_per_product": round(total_rows / max(unique_products, 1), 2),
        "items_per_product": round(unique_items / max(unique_products, 1), 2),
        "vendors_per_product": round(unique_vendors / max(unique_products, 1), 2),
    }

    # productId별 itemId 수 분포 → 옵션/변형 구조
    pid_to_items = df_work.groupby("product_id")["item_id"].nunique()
    multi_item = pid_to_items[pid_to_items > 1]

    result["catalog_structure"] = {
        "single_item_products": int((pid_to_items == 1).sum()),
        "multi_item_products": int(len(multi_item)),
        "max_items_per_product": int(pid_to_items.max()) if not pid_to_items.empty else 0,
        "avg_items_per_product": round(float(pid_to_items.mean()), 2) if not pid_to_items.empty else 0,
    }

    # productId별 vendorItemId 수 → 멀티셀러 구조
    pid_to_vendors = df_work.groupby("product_id")["vendor_item_id"].nunique()
    multi_vendor = pid_to_vendors[pid_to_vendors > 1]

    result["catalog_structure"]["multi_seller_products"] = int(len(multi_vendor))
    result["catalog_structure"]["max_sellers_per_product"] = int(pid_to_vendors.max()) if not pid_to_vendors.empty else 0

    # 멀티셀러 상품 상세 (같은 productId에 다른 셀러가 있는 경우)
    if not multi_vendor.empty:
        for pid in multi_vendor.head(10).index:
            rows = df_work[df_work["product_id"] == pid]
            sellers = rows["vendor_item_id"].unique().tolist()
            result["multi_seller_products"].append({
                "product_id": str(pid),
                "product_name": str(rows.iloc[0]["product_name"])[:50],
                "seller_count": len(sellers),
                "vendor_item_ids": sellers[:5],
                "prices": rows["sale_price"].dropna().tolist()[:5],
            })

    # 멀티아이템 상품 (옵션/변형)
    if not multi_item.empty:
        for pid in multi_item.head(10).index:
            rows = df_work[df_work["product_id"] == pid]
            items = rows["item_id"].unique().tolist()
            result["multi_item_products"].append({
                "product_id": str(pid),
                "product_name": str(rows.iloc[0]["product_name"])[:50],
                "item_count": len(items),
                "item_ids": items[:5],
            })

    # vendorItemId 패턴 분석 (숫자 구조)
    vid_lengths = df_work["vendor_item_id"].astype(str).str.len()
    result["id_counts"]["vendor_id_avg_length"] = round(float(vid_lengths.mean()), 1)
    result["id_counts"]["vendor_id_length_range"] = f"{int(vid_lengths.min())}~{int(vid_lengths.max())}"

    return result


# ══════════════════════════════════════════════
# 3. 순위 결정 심층 역공학
# ══════════════════════════════════════════════

def reverse_ranking_deep(df: pd.DataFrame) -> dict:
    """단순 상관을 넘어 비선형 효과, 임계값, 상호작용 항을 탐지.

    - GradientBoosting Partial Dependence → 각 팩터의 비선형 영향
    - 임계값 탐지 (리뷰 N개 이상이면 급격히 순위 상승)
    - 팩터 간 상호작용 (리뷰×가격, 평점×배송 등)
    """
    result = {
        "nonlinear_effects": {},
        "thresholds": {},
        "interactions": {},
        "factor_ranking": [],
        "model_performance": {},
    }

    organic = df[df["ad_type"] == "자연검색"].copy()
    if len(organic) < 15:
        result["note"] = f"자연검색 상품 {len(organic)}개 — 최소 15개 필요"
        return result

    # 피처 준비
    organic["discount_pct"] = organic["discount_rate"].apply(
        lambda x: int(re.search(r'(\d+)', str(x)).group(1)) if pd.notna(x) and re.search(r'(\d+)', str(x)) else 0
    )
    organic["is_rocket"] = organic["delivery_type"].isin(["로켓배송", "로켓직구", "로켓럭셔리"]).astype(int)
    organic["has_cashback"] = (organic["cashback"].fillna(0) > 0).astype(int)
    organic["name_length"] = organic["product_name"].str.len().fillna(0)
    organic["log_review"] = np.log1p(organic["review_count"].fillna(0))
    organic["log_price"] = np.log1p(organic["sale_price"].fillna(0))

    feature_cols = ["review_count", "rating", "sale_price", "discount_pct",
                    "is_rocket", "has_cashback", "name_length", "log_review"]
    feature_labels = ["리뷰수", "평점", "판매가", "할인율",
                      "로켓배송", "적립금유무", "상품명길이", "log(리뷰)"]

    reg_df = organic[["organic_rank"] + feature_cols].dropna()
    if len(reg_df) < 15:
        result["note"] = f"유효 데이터 {len(reg_df)}개 — 최소 15개 필요"
        return result

    X = reg_df[feature_cols].values
    y = reg_df["organic_rank"].values

    # ── GradientBoosting (비선형 포착) ──
    gb = GradientBoostingRegressor(
        n_estimators=200, max_depth=4, learning_rate=0.1,
        subsample=0.8, random_state=42,
    )
    gb.fit(X, y)

    r2 = gb.score(X, y)
    result["model_performance"] = {
        "model": "GradientBoosting",
        "r_squared": round(float(r2), 4),
        "n_samples": len(reg_df),
        "interpretation": "순위 변동의 {:.0%}를 설명".format(r2),
    }

    # 피처 중요도
    importances = gb.feature_importances_
    sorted_idx = np.argsort(importances)[::-1]
    result["factor_ranking"] = [
        {"factor": feature_labels[i], "importance": round(float(importances[i]), 4),
         "pct": f"{importances[i]*100:.1f}%"}
        for i in sorted_idx
    ]

    # ── Partial Dependence (각 팩터의 비선형 효과) ──
    for idx, (col, label) in enumerate(zip(feature_cols, feature_labels)):
        if importances[idx] < 0.02:  # 중요도 2% 미만은 건너뜀
            continue
        try:
            pd_result = partial_dependence(gb, X, features=[idx], kind="average",
                                            grid_resolution=20)
            values = pd_result["grid_values"][0]
            effects = pd_result["average"][0]

            # 비선형성 판단: 선형 근사 대비 잔차
            if len(values) > 2:
                linear_approx = np.linspace(effects[0], effects[-1], len(effects))
                nonlinearity = np.mean(np.abs(effects - linear_approx))
                effect_range = np.max(effects) - np.min(effects)
                nonlinear_ratio = nonlinearity / max(effect_range, 0.001)

                result["nonlinear_effects"][label] = {
                    "effect_range": round(float(effect_range), 2),
                    "nonlinearity": round(float(nonlinear_ratio), 3),
                    "is_nonlinear": nonlinear_ratio > 0.15,
                    "direction": "순위 상승" if effects[-1] < effects[0] else "순위 하락",
                    "curve_points": [
                        {"x": round(float(v), 1), "y": round(float(e), 2)}
                        for v, e in zip(values[::4], effects[::4])  # 5개 포인트
                    ],
                }
        except Exception:
            pass

    # ── 임계값 탐지 ──
    # 리뷰수 임계값: 리뷰수를 구간으로 나눠 각 구간의 평균 순위를 비교
    for col, label in [("review_count", "리뷰수"), ("rating", "평점"), ("sale_price", "판매가")]:
        col_data = reg_df[col].dropna()
        if len(col_data) < 10:
            continue

        # 4분위로 나눠서 순위 차이 확인
        try:
            quartiles = pd.qcut(col_data, 4, duplicates="drop")
            group_means = reg_df.groupby(quartiles)["organic_rank"].mean()

            if len(group_means) >= 2:
                # 가장 큰 순위 점프가 일어나는 구간 = 임계값
                jumps = []
                prev_mean = None
                for interval, mean_rank in group_means.items():
                    if prev_mean is not None:
                        jumps.append({
                            "boundary": str(interval),
                            "rank_jump": round(float(prev_mean - mean_rank), 1),
                        })
                    prev_mean = mean_rank

                if jumps:
                    biggest_jump = max(jumps, key=lambda j: abs(j["rank_jump"]))
                    result["thresholds"][label] = {
                        "quartile_means": {str(k): round(float(v), 1) for k, v in group_means.items()},
                        "biggest_jump": biggest_jump,
                        "interpretation": f"{label}이(가) {biggest_jump['boundary']} 구간에서 순위 {abs(biggest_jump['rank_jump']):.1f} 점프",
                    }
        except Exception:
            pass

    # ── 상호작용 효과 ──
    interaction_pairs = [
        ("review_count", "sale_price", "리뷰×가격"),
        ("rating", "is_rocket", "평점×로켓배송"),
        ("review_count", "is_rocket", "리뷰×로켓배송"),
        ("sale_price", "discount_pct", "가격×할인"),
    ]

    for col1, col2, label in interaction_pairs:
        if col1 not in feature_cols or col2 not in feature_cols:
            continue
        idx1 = feature_cols.index(col1)
        idx2 = feature_cols.index(col2)

        try:
            pd_2d = partial_dependence(gb, X, features=[(idx1, idx2)],
                                        kind="average", grid_resolution=10)
            effects_2d = pd_2d["average"][0]
            # 상호작용 강도: 2D 효과의 분산
            interaction_strength = float(np.std(effects_2d))
            main_effect_1 = float(np.std(partial_dependence(gb, X, features=[idx1],
                                                             kind="average", grid_resolution=10)["average"][0]))
            main_effect_2 = float(np.std(partial_dependence(gb, X, features=[idx2],
                                                             kind="average", grid_resolution=10)["average"][0]))
            # 주효과 대비 상호작용 비율
            main_sum = main_effect_1 + main_effect_2
            if main_sum > 0:
                interaction_ratio = interaction_strength / main_sum
                result["interactions"][label] = {
                    "strength": round(interaction_strength, 3),
                    "ratio": round(interaction_ratio, 3),
                    "significant": interaction_ratio > 0.3,
                }
        except Exception:
            pass

    return result


# ══════════════════════════════════════════════
# 4. sourceType 해독
# ══════════════════════════════════════════════

def reverse_source_type(df: pd.DataFrame) -> dict:
    """sourceType 값별 특성을 분석하여 의미 추론.

    sourceType이 쿠팡 내부의 어떤 "출처/경로" 분류인지 해독.
    """
    result = {"types": {}, "interpretation": {}}

    if df.empty or "source_type" not in df.columns:
        return result

    types = df["source_type"].value_counts()

    for st, count in types.items():
        subset = df[df["source_type"] == st]
        organic_subset = subset[subset["ad_type"] == "자연검색"]

        type_info = {
            "count": int(count),
            "share_pct": round(count / len(df) * 100, 1),
            "ad_ratio": round((subset["ad_type"] == "AD").sum() / max(len(subset), 1) * 100, 1),
            "avg_rank": round(float(organic_subset["organic_rank"].mean()), 1) if not organic_subset.empty else None,
            "avg_price": round(float(subset["sale_price"].mean()), 0) if subset["sale_price"].notna().any() else None,
            "avg_reviews": round(float(subset["review_count"].mean()), 0) if subset["review_count"].notna().any() else None,
            "rocket_ratio": round(
                subset["delivery_type"].isin(["로켓배송", "로켓직구", "로켓럭셔리"]).sum() / max(len(subset), 1) * 100, 1
            ),
        }
        result["types"][str(st)] = type_info

    # 의미 추론
    for st, info in result["types"].items():
        hints = []
        if info["ad_ratio"] > 80:
            hints.append("광고 전용 소스")
        if info["rocket_ratio"] > 80:
            hints.append("로켓배송 전용")
        if info["avg_rank"] and info["avg_rank"] < 10:
            hints.append("상위 노출 소스")
        if "sdp" in str(st).lower():
            hints.append("추정: 검색 결과 페이지(Search Display Product)")
        if "rec" in str(st).lower():
            hints.append("추정: 추천 시스템")
        if "catalog" in str(st).lower():
            hints.append("추정: 카탈로그 기반")

        result["interpretation"][str(st)] = hints if hints else ["분류 불명 — 데이터 추가 필요"]

    return result


# ══════════════════════════════════════════════
# 5. 배송 유형 부스트 정량화
# ══════════════════════════════════════════════

def reverse_delivery_boost(df: pd.DataFrame) -> dict:
    """로켓배송이 순위에 미치는 '부스트'를 정량화.

    같은 가격대/리뷰수 구간에서 로켓 vs 마켓플레이스의 순위 차이를 측정.
    → 배송 유형에 의한 순수 순위 효과 추출.
    """
    result = {"overall": {}, "controlled": {}, "statistical_test": {}}

    organic = df[df["ad_type"] == "자연검색"].copy()
    if len(organic) < 10:
        return result

    organic["is_rocket"] = organic["delivery_type"].isin(["로켓배송", "로켓직구", "로켓럭셔리"]).astype(int)

    rocket = organic[organic["is_rocket"] == 1]["organic_rank"].dropna()
    marketplace = organic[organic["is_rocket"] == 0]["organic_rank"].dropna()

    result["overall"] = {
        "rocket_count": int(len(rocket)),
        "marketplace_count": int(len(marketplace)),
        "rocket_avg_rank": round(float(rocket.mean()), 1) if not rocket.empty else None,
        "marketplace_avg_rank": round(float(marketplace.mean()), 1) if not marketplace.empty else None,
        "rank_advantage": round(float(marketplace.mean() - rocket.mean()), 1) if not rocket.empty and not marketplace.empty else None,
    }

    # Mann-Whitney 검정
    if len(rocket) >= 3 and len(marketplace) >= 3:
        stat, pval = stats.mannwhitneyu(rocket, marketplace, alternative="less")
        result["statistical_test"] = {
            "test": "Mann-Whitney U",
            "statistic": round(float(stat), 2),
            "p_value": round(float(pval), 4),
            "significant": pval < 0.05,
            "conclusion": "로켓배송이 유의미하게 높은 순위" if pval < 0.05 else "통계적으로 유의미하지 않음",
        }

    # 가격/리뷰 통제 후 비교 (유사 조건에서의 순수 배송 효과)
    organic["price_tier"] = pd.qcut(organic["sale_price"].fillna(0), 3, labels=["저가", "중가", "고가"], duplicates="drop")
    organic["review_tier"] = pd.cut(organic["review_count"].fillna(0), bins=[-1, 10, 100, float("inf")],
                                     labels=["소수(<10)", "보통(10-100)", "다수(100+)"])

    controlled_results = []
    for price_t in organic["price_tier"].unique():
        for review_t in organic["review_tier"].unique():
            mask = (organic["price_tier"] == price_t) & (organic["review_tier"] == review_t)
            subset = organic[mask]
            r = subset[subset["is_rocket"] == 1]["organic_rank"].dropna()
            m = subset[subset["is_rocket"] == 0]["organic_rank"].dropna()
            if len(r) >= 2 and len(m) >= 2:
                controlled_results.append({
                    "price_tier": str(price_t),
                    "review_tier": str(review_t),
                    "rocket_avg_rank": round(float(r.mean()), 1),
                    "marketplace_avg_rank": round(float(m.mean()), 1),
                    "boost": round(float(m.mean() - r.mean()), 1),
                    "n_rocket": len(r),
                    "n_marketplace": len(m),
                })

    result["controlled"] = controlled_results

    if controlled_results:
        avg_boost = np.mean([c["boost"] for c in controlled_results])
        result["overall"]["controlled_avg_boost"] = round(float(avg_boost), 1)
        result["overall"]["boost_interpretation"] = (
            f"가격/리뷰 통제 후 로켓배송의 평균 순위 이점: {abs(avg_boost):.1f}위"
            if avg_boost > 0 else "통제 후 유의미한 부스트 없음"
        )

    return result


# ══════════════════════════════════════════════
# 6. 가격 알고리즘 역공학
# ══════════════════════════════════════════════

def reverse_price_algorithm(df: pd.DataFrame) -> dict:
    """쿠팡이 가격을 어떻게 순위에 반영하는지 분석.

    - 가격-순위 관계 (선형? 비선형? U자?)
    - '최적 가격 구간' (sweet spot)
    - 가격 이상치 패널티 여부
    - 할인율의 독립적 효과
    """
    result = {"price_rank_curve": {}, "sweet_spot": {}, "outlier_penalty": {}, "discount_effect": {}}

    organic = df[df["ad_type"] == "자연검색"].copy()
    prices = organic["sale_price"].dropna()
    if len(prices) < 10:
        return result

    # ── 가격 구간별 평균 순위 (비선형 감지) ──
    try:
        organic["price_decile"] = pd.qcut(organic["sale_price"].dropna(), 10, duplicates="drop")
        curve = organic.groupby("price_decile")["organic_rank"].agg(["mean", "count"])

        price_curve = []
        for interval, row in curve.iterrows():
            price_curve.append({
                "range": str(interval),
                "avg_rank": round(float(row["mean"]), 1),
                "count": int(row["count"]),
            })
        result["price_rank_curve"] = price_curve

        # sweet spot: 가장 낮은 평균 순위를 가진 구간
        if price_curve:
            best = min(price_curve, key=lambda x: x["avg_rank"])
            result["sweet_spot"] = {
                "best_price_range": best["range"],
                "avg_rank_in_range": best["avg_rank"],
                "interpretation": f"가격 {best['range']}에서 평균 순위 {best['avg_rank']}위로 최고",
            }
    except Exception:
        pass

    # ── 가격 이상치 패널티 ──
    median_price = float(prices.median())
    if median_price > 0:
        organic["price_ratio"] = organic["sale_price"] / median_price
        # 중앙값 대비 2배 이상 비싼 상품 vs 0.5배 이하 저렴한 상품의 순위
        expensive = organic[organic["price_ratio"] > 2]["organic_rank"].dropna()
        cheap = organic[organic["price_ratio"] < 0.5]["organic_rank"].dropna()
        normal = organic[(organic["price_ratio"] >= 0.5) & (organic["price_ratio"] <= 2)]["organic_rank"].dropna()

        result["outlier_penalty"] = {
            "median_price": round(median_price, 0),
            "expensive_avg_rank": round(float(expensive.mean()), 1) if not expensive.empty else None,
            "cheap_avg_rank": round(float(cheap.mean()), 1) if not cheap.empty else None,
            "normal_avg_rank": round(float(normal.mean()), 1) if not normal.empty else None,
            "expensive_count": len(expensive),
            "cheap_count": len(cheap),
            "normal_count": len(normal),
        }

        # 패널티 존재 여부
        if not expensive.empty and not normal.empty:
            if expensive.mean() > normal.mean() + 5:
                result["outlier_penalty"]["penalty_detected"] = "고가 패널티 감지 (중앙값 2배 이상 → 순위 하락)"
        if not cheap.empty and not normal.empty:
            if cheap.mean() > normal.mean() + 5:
                result["outlier_penalty"]["cheap_penalty"] = "저가도 불리 (품질 신호?)"

    # ── 할인율 독립 효과 ──
    organic["discount_pct"] = organic["discount_rate"].apply(
        lambda x: int(re.search(r'(\d+)', str(x)).group(1)) if pd.notna(x) and re.search(r'(\d+)', str(x)) else 0
    )
    discounted = organic[organic["discount_pct"] > 0]["organic_rank"].dropna()
    no_discount = organic[organic["discount_pct"] == 0]["organic_rank"].dropna()

    if len(discounted) >= 3 and len(no_discount) >= 3:
        stat, pval = stats.mannwhitneyu(discounted, no_discount, alternative="less")
        result["discount_effect"] = {
            "discounted_avg_rank": round(float(discounted.mean()), 1),
            "no_discount_avg_rank": round(float(no_discount.mean()), 1),
            "p_value": round(float(pval), 4),
            "significant": pval < 0.05,
            "conclusion": "할인 상품이 유의미하게 높은 순위" if pval < 0.05 else "할인율의 독립적 효과 불분명",
        }

    return result


# ══════════════════════════════════════════════
# 7. 시간 패턴 역공학 (다중 스냅샷)
# ══════════════════════════════════════════════

def reverse_time_patterns(keyword: str, db: CoupangDB) -> dict:
    """여러 스냅샷에서 시간에 따른 알고리즘 패턴을 추출.

    - 순위 안정성 (장기 상위 유지 상품의 공통점)
    - 신규 진입 패턴 (새 상품이 어디에 처음 나타나는가)
    - 순위 관성 (한번 올라가면 얼마나 유지되는가)
    """
    result = {"stability_analysis": {}, "entry_patterns": {}, "rank_inertia": {},
              "snapshot_count": 0}

    kw_row = db.conn.execute("SELECT id FROM keywords WHERE keyword = ?", (keyword,)).fetchone()
    if not kw_row:
        result["note"] = "키워드 없음"
        return result

    keyword_id = kw_row["id"]
    snapshots = db.conn.execute(
        "SELECT id, collected_at FROM snapshots WHERE keyword_id = ? ORDER BY collected_at",
        (keyword_id,),
    ).fetchall()

    result["snapshot_count"] = len(snapshots)
    if len(snapshots) < 2:
        result["note"] = f"스냅샷 {len(snapshots)}개 — 시계열 분석에 2개 이상 필요. 데이터를 더 수집하세요."
        return result

    # 각 스냅샷의 product_id → rank 매핑
    all_ranks = {}
    for snap in snapshots:
        rows = db.conn.execute(
            """SELECT product_id, organic_rank, review_count, sale_price, delivery_type
               FROM products WHERE snapshot_id = ? AND ad_type = '자연검색' AND organic_rank IS NOT NULL""",
            (snap["id"],),
        ).fetchall()
        all_ranks[snap["collected_at"]] = {r["product_id"]: dict(r) for r in rows}

    dates = list(all_ranks.keys())

    # ── 안정성 분석: 모든 스냅샷에 나타나는 상품 ──
    all_product_ids = [set(ranks.keys()) for ranks in all_ranks.values()]
    always_present = set.intersection(*all_product_ids) if all_product_ids else set()
    ever_present = set.union(*all_product_ids) if all_product_ids else set()

    result["stability_analysis"] = {
        "always_present_count": len(always_present),
        "ever_present_count": len(ever_present),
        "stability_ratio": round(len(always_present) / max(len(ever_present), 1), 3),
    }

    # 항상 상위에 있는 상품의 공통 특성
    if always_present and len(dates) >= 2:
        latest = all_ranks[dates[-1]]
        stable_products = []
        for pid in always_present:
            if pid in latest:
                info = latest[pid]
                ranks_over_time = [all_ranks[d].get(pid, {}).get("organic_rank") for d in dates]
                ranks_over_time = [r for r in ranks_over_time if r is not None]
                stable_products.append({
                    "product_id": pid,
                    "avg_rank": round(np.mean(ranks_over_time), 1),
                    "rank_stddev": round(np.std(ranks_over_time), 1),
                    "review_count": info.get("review_count"),
                    "sale_price": info.get("sale_price"),
                    "delivery_type": info.get("delivery_type"),
                })

        stable_products.sort(key=lambda x: x["avg_rank"])
        result["stability_analysis"]["top_stable_products"] = stable_products[:10]

        # 안정 상위 상품의 공통 특성
        if stable_products:
            top_stable = [p for p in stable_products if p["avg_rank"] <= 20]
            if top_stable:
                avg_reviews = np.mean([p["review_count"] or 0 for p in top_stable])
                avg_price = np.mean([p["sale_price"] or 0 for p in top_stable])
                rocket_pct = sum(1 for p in top_stable if p["delivery_type"] in ["로켓배송", "로켓직구", "로켓럭셔리"]) / len(top_stable) * 100
                result["stability_analysis"]["top_stable_traits"] = {
                    "count": len(top_stable),
                    "avg_reviews": round(float(avg_reviews), 0),
                    "avg_price": round(float(avg_price), 0),
                    "rocket_pct": round(float(rocket_pct), 1),
                    "avg_rank_volatility": round(float(np.mean([p["rank_stddev"] for p in top_stable])), 1),
                }

    # ── 신규 진입 패턴 ──
    if len(dates) >= 2:
        new_entries = []
        for i in range(1, len(dates)):
            prev_ids = set(all_ranks[dates[i-1]].keys())
            curr_ids = set(all_ranks[dates[i]].keys())
            new_ids = curr_ids - prev_ids
            for pid in new_ids:
                info = all_ranks[dates[i]][pid]
                new_entries.append({
                    "date": dates[i][:10],
                    "product_id": pid,
                    "entry_rank": info.get("organic_rank"),
                    "review_count": info.get("review_count"),
                    "delivery_type": info.get("delivery_type"),
                })

        if new_entries:
            entry_ranks = [e["entry_rank"] for e in new_entries if e["entry_rank"]]
            result["entry_patterns"] = {
                "total_new_entries": len(new_entries),
                "avg_entry_rank": round(float(np.mean(entry_ranks)), 1) if entry_ranks else None,
                "median_entry_rank": round(float(np.median(entry_ranks)), 1) if entry_ranks else None,
                "entered_top10_count": len([r for r in entry_ranks if r <= 10]),
                "samples": new_entries[:10],
            }

    # ── 순위 관성 ──
    if len(dates) >= 3 and always_present:
        # 상승 후 유지되는 비율
        sustained_ups = 0
        total_ups = 0
        for pid in always_present:
            ranks = [all_ranks[d].get(pid, {}).get("organic_rank") for d in dates]
            ranks = [r for r in ranks if r is not None]
            if len(ranks) < 3:
                continue
            for i in range(1, len(ranks) - 1):
                if ranks[i] < ranks[i-1]:  # 순위 상승 (숫자 감소)
                    total_ups += 1
                    if ranks[i+1] <= ranks[i] + 3:  # 다음에도 유지
                        sustained_ups += 1

        if total_ups > 0:
            result["rank_inertia"] = {
                "total_rank_ups": total_ups,
                "sustained_count": sustained_ups,
                "inertia_ratio": round(sustained_ups / total_ups, 3),
                "interpretation": f"순위 상승 후 유지 확률: {sustained_ups/total_ups*100:.0f}%",
            }

    return result


# ══════════════════════════════════════════════
# 8. 셀러 컨트롤 요소 분석
# ══════════════════════════════════════════════

def reverse_seller_factors(df: pd.DataFrame, keyword: str = None, db: CoupangDB = None) -> dict:
    """셀러가 컨트롤 가능한 요소별 노출 영향도 분석.

    검색어, 카탈로그 매칭, 광고, 할인율, 리뷰, 가격 포지셔닝, 적립금의
    실제 순위 영향을 Mann-Whitney U / Spearman 상관으로 정량화한다.
    """
    result = {
        "keyword_effect": {},
        "catalog_effect": {},
        "option_effect": {},
        "ad_effect": {},
        "review_effect": {},
        "discount_effect": {},
        "cashback_effect": {},
        "price_positioning": {},
        "factor_summary": [],
    }

    organic = df[df["ad_type"] == "자연검색"].copy()
    ads = df[df["ad_type"] == "AD"]

    if len(organic) < 5:
        result["note"] = f"자연검색 상품 {len(organic)}개 — 최소 5개 필요"
        return result

    # ── 유틸: Mann-Whitney 테스트 ──
    def _mw_test(group_a, group_b, alt="less"):
        """group_a가 group_b보다 순위가 높은지(숫자가 작은지) 검정."""
        if len(group_a) < 3 or len(group_b) < 3:
            return None
        stat, pval = stats.mannwhitneyu(group_a, group_b, alternative=alt)
        return {"p_value": round(float(pval), 4), "significant": pval < 0.05}

    # ── [1] 검색어 효과 ──
    if "keyword_in_name" in organic.columns:
        has_kw = organic[organic["keyword_in_name"] == 1]["organic_rank"].dropna()
        no_kw = organic[organic["keyword_in_name"] == 0]["organic_rank"].dropna()

        kw_effect = {
            "has_keyword_avg_rank": round(float(has_kw.mean()), 1) if len(has_kw) > 0 else None,
            "no_keyword_avg_rank": round(float(no_kw.mean()), 1) if len(no_kw) > 0 else None,
            "has_keyword_count": len(has_kw),
            "no_keyword_count": len(no_kw),
        }

        if len(has_kw) > 0 and len(no_kw) > 0:
            kw_effect["rank_diff"] = round(float(no_kw.mean() - has_kw.mean()), 1)
            test = _mw_test(has_kw, no_kw)
            if test:
                kw_effect.update(test)

        # 키워드 위치 (앞 vs 뒤)
        if "keyword_position" in organic.columns:
            kp = organic.copy()
            kp["keyword_position"] = pd.to_numeric(kp["keyword_position"], errors="coerce")
            kp = kp[kp["keyword_position"].notna() & (kp["keyword_position"] > 0)]
            if len(kp) > 5:
                median_pos = kp["keyword_position"].median()
                front = kp[kp["keyword_position"] <= median_pos]["organic_rank"].dropna()
                back = kp[kp["keyword_position"] > median_pos]["organic_rank"].dropna()
                kw_effect["position"] = {
                    "front_avg_rank": round(float(front.mean()), 1) if len(front) > 0 else None,
                    "back_avg_rank": round(float(back.mean()), 1) if len(back) > 0 else None,
                    "front_count": len(front),
                    "back_count": len(back),
                }

        # 상품명 길이
        organic["name_len"] = organic["product_name"].str.len().fillna(0)
        if organic["name_len"].nunique() > 3:
            q33 = int(organic["name_len"].quantile(0.33))
            q66 = int(organic["name_len"].quantile(0.66))
            short = organic[organic["name_len"] <= q33]["organic_rank"].dropna()
            medium = organic[(organic["name_len"] > q33) & (organic["name_len"] <= q66)]["organic_rank"].dropna()
            long_n = organic[organic["name_len"] > q66]["organic_rank"].dropna()
            kw_effect["name_length"] = {
                "short": {"max_len": q33, "avg_rank": round(float(short.mean()), 1), "count": len(short)},
                "medium": {"range": f"{q33}~{q66}", "avg_rank": round(float(medium.mean()), 1), "count": len(medium)},
                "long": {"min_len": q66, "avg_rank": round(float(long_n.mean()), 1), "count": len(long_n)},
            }

        result["keyword_effect"] = kw_effect

    # ── [2] 카탈로그 매칭 ──
    pid_counts = df.groupby("product_id")["vendor_item_id"].nunique()
    multi_pids = set(pid_counts[pid_counts > 1].index)
    single_pids = set(pid_counts[pid_counts == 1].index)
    multi = organic[organic["product_id"].isin(multi_pids)]["organic_rank"].dropna()
    single = organic[organic["product_id"].isin(single_pids)]["organic_rank"].dropna()

    cat_effect = {
        "multi_seller_avg_rank": round(float(multi.mean()), 1) if len(multi) > 0 else None,
        "single_seller_avg_rank": round(float(single.mean()), 1) if len(single) > 0 else None,
        "multi_seller_count": len(multi),
        "single_seller_count": len(single),
    }
    if len(multi) > 0 and len(single) > 0:
        cat_effect["rank_diff"] = round(float(single.mean() - multi.mean()), 1)
        test = _mw_test(multi, single)
        if test:
            cat_effect.update(test)

        # 왜 유리한지: 리뷰수 비교
        multi_products = organic[organic["product_id"].isin(multi_pids)]
        single_products = organic[organic["product_id"].isin(single_pids)]
        cat_effect["multi_avg_reviews"] = round(float(multi_products["review_count"].mean()), 0)
        cat_effect["single_avg_reviews"] = round(float(single_products["review_count"].mean()), 0)

    result["catalog_effect"] = cat_effect

    # ── [3] 옵션 수 ──
    item_counts = df.groupby("product_id")["item_id"].nunique()
    multi_items = set(item_counts[item_counts > 1].index)
    single_items = set(item_counts[item_counts == 1].index)
    multi_opt = organic[organic["product_id"].isin(multi_items)]["organic_rank"].dropna()
    single_opt = organic[organic["product_id"].isin(single_items)]["organic_rank"].dropna()

    result["option_effect"] = {
        "multi_option_avg_rank": round(float(multi_opt.mean()), 1) if len(multi_opt) > 0 else None,
        "single_option_avg_rank": round(float(single_opt.mean()), 1) if len(single_opt) > 0 else None,
        "multi_option_count": len(multi_opt),
        "single_option_count": len(single_opt),
        "multi_product_count": len(multi_items),
        "single_product_count": len(single_items),
    }

    # ── [4] 광고 효과 ──
    ad_eff = {
        "ad_count": len(ads),
        "organic_count": len(organic),
        "ad_ratio": round(len(ads) / max(len(df), 1) * 100, 1),
    }

    if not ads.empty:
        top3 = int((ads["exposure_order"] <= 3).sum())
        mid = int(((ads["exposure_order"] > 3) & (ads["exposure_order"] <= 12)).sum())
        bottom = int((ads["exposure_order"] > 12).sum())
        ad_eff["position_dist"] = {"top3": top3, "mid_4_12": mid, "bottom_13plus": bottom}
        ad_eff["avg_exposure_order"] = round(float(ads["exposure_order"].mean()), 1)

    # 광고+자연 동시 노출
    ad_pids = set(ads["product_id"].dropna().unique())
    organic_pids = set(organic["product_id"].dropna().unique())
    overlap = ad_pids & organic_pids

    ad_eff["overlap_count"] = len(overlap)
    if overlap:
        ol_rank = organic[organic["product_id"].isin(overlap)]["organic_rank"].dropna()
        no_rank = organic[~organic["product_id"].isin(overlap)]["organic_rank"].dropna()
        if len(ol_rank) > 0 and len(no_rank) > 0:
            ad_eff["with_ad_avg_rank"] = round(float(ol_rank.mean()), 1)
            ad_eff["without_ad_avg_rank"] = round(float(no_rank.mean()), 1)
            ad_eff["rank_diff"] = round(float(no_rank.mean() - ol_rank.mean()), 1)
            test = _mw_test(ol_rank, no_rank)
            if test:
                ad_eff.update(test)

    result["ad_effect"] = ad_eff

    # ── [5] 리뷰 수 ──
    reviews = organic[["review_count", "organic_rank"]].dropna()
    rev_eff = {"count": len(reviews)}

    if len(reviews) > 10:
        bins = [0, 5, 20, 50, 100, float("inf")]
        labels = ["0~5", "6~20", "21~50", "51~100", "100+"]
        reviews = reviews.copy()
        reviews["tier"] = pd.cut(reviews["review_count"], bins=bins, labels=labels, include_lowest=True)
        tiers = {}
        for tier in labels:
            subset = reviews[reviews["tier"] == tier]["organic_rank"]
            if len(subset) > 0:
                tiers[tier] = {"avg_rank": round(float(subset.mean()), 1), "count": len(subset)}
        rev_eff["tiers"] = tiers

        corr, pval = stats.spearmanr(reviews["review_count"], reviews["organic_rank"])
        rev_eff["spearman_r"] = round(float(corr), 3)
        rev_eff["spearman_p"] = round(float(pval), 4)
        rev_eff["direction"] = "리뷰 많을수록 순위 상승" if corr < 0 else "리뷰와 순위 무관"
        rev_eff["strength"] = "강함" if abs(corr) > 0.5 else "보통" if abs(corr) > 0.3 else "약함"

    result["review_effect"] = rev_eff

    # ── [6] 할인율 ──
    organic["discount_pct"] = organic["discount_rate"].apply(
        lambda x: int(re.search(r"(\d+)", str(x)).group(1)) if pd.notna(x) and re.search(r"(\d+)", str(x)) else 0
    )
    has_disc = organic[organic["discount_pct"] > 0]["organic_rank"].dropna()
    no_disc = organic[organic["discount_pct"] == 0]["organic_rank"].dropna()

    disc_eff = {
        "discounted_avg_rank": round(float(has_disc.mean()), 1) if len(has_disc) > 0 else None,
        "no_discount_avg_rank": round(float(no_disc.mean()), 1) if len(no_disc) > 0 else None,
        "discounted_count": len(has_disc),
        "no_discount_count": len(no_disc),
    }
    if len(has_disc) > 0 and len(no_disc) > 0:
        disc_eff["rank_diff"] = round(float(no_disc.mean() - has_disc.mean()), 1)
        test = _mw_test(has_disc, no_disc)
        if test:
            disc_eff.update(test)

    result["discount_effect"] = disc_eff

    # ── [7] 적립금 ──
    if "cashback" in organic.columns:
        has_cb = organic[organic["cashback"].fillna(0) > 0]["organic_rank"].dropna()
        no_cb = organic[organic["cashback"].fillna(0) == 0]["organic_rank"].dropna()
        cb_eff = {
            "with_cashback_avg_rank": round(float(has_cb.mean()), 1) if len(has_cb) > 0 else None,
            "no_cashback_avg_rank": round(float(no_cb.mean()), 1) if len(no_cb) > 0 else None,
            "with_cashback_count": len(has_cb),
            "no_cashback_count": len(no_cb),
        }
        if len(has_cb) > 0 and len(no_cb) > 0:
            cb_eff["rank_diff"] = round(float(no_cb.mean() - has_cb.mean()), 1)
            test = _mw_test(has_cb, no_cb)
            if test:
                cb_eff.update(test)
        result["cashback_effect"] = cb_eff

    # ── [8] 가격 포지셔닝 ──
    prices = organic["sale_price"].dropna()
    if len(prices) >= 5:
        median_p = float(prices.median())
        if median_p > 0:
            organic["price_ratio"] = organic["sale_price"] / median_p
            cheap = organic[organic["price_ratio"] < 0.8]["organic_rank"].dropna()
            normal = organic[(organic["price_ratio"] >= 0.8) & (organic["price_ratio"] <= 1.2)]["organic_rank"].dropna()
            expensive = organic[organic["price_ratio"] > 1.2]["organic_rank"].dropna()

            result["price_positioning"] = {
                "median_price": round(median_p, 0),
                "cheap_avg_rank": round(float(cheap.mean()), 1) if len(cheap) > 0 else None,
                "normal_avg_rank": round(float(normal.mean()), 1) if len(normal) > 0 else None,
                "expensive_avg_rank": round(float(expensive.mean()), 1) if len(expensive) > 0 else None,
                "cheap_count": len(cheap),
                "normal_count": len(normal),
                "expensive_count": len(expensive),
            }

    # ── 종합 영향도 랭킹 ──
    factors = []

    kw_e = result["keyword_effect"]
    if kw_e.get("rank_diff") is not None:
        factors.append({"factor": "검색어(상품명)", "rank_diff": kw_e["rank_diff"],
                        "significant": kw_e.get("significant", False), "p_value": kw_e.get("p_value")})

    cat_e = result["catalog_effect"]
    if cat_e.get("rank_diff") is not None:
        factors.append({"factor": "카탈로그 매칭", "rank_diff": cat_e["rank_diff"],
                        "significant": cat_e.get("significant", False), "p_value": cat_e.get("p_value")})

    ad_e = result["ad_effect"]
    if ad_e.get("rank_diff") is not None:
        factors.append({"factor": "광고(간접효과)", "rank_diff": ad_e["rank_diff"],
                        "significant": ad_e.get("significant", False), "p_value": ad_e.get("p_value")})

    disc_e = result["discount_effect"]
    if disc_e.get("rank_diff") is not None:
        factors.append({"factor": "할인율", "rank_diff": disc_e["rank_diff"],
                        "significant": disc_e.get("significant", False), "p_value": disc_e.get("p_value")})

    cb_e = result.get("cashback_effect", {})
    if cb_e.get("rank_diff") is not None:
        factors.append({"factor": "적립금", "rank_diff": cb_e["rank_diff"],
                        "significant": cb_e.get("significant", False), "p_value": cb_e.get("p_value")})

    pp = result["price_positioning"]
    if pp.get("normal_avg_rank") is not None and pp.get("cheap_avg_rank") is not None:
        factors.append({"factor": "가격 포지셔닝", "rank_diff": round(pp["cheap_avg_rank"] - pp["normal_avg_rank"], 1),
                        "significant": None, "p_value": None})

    rev_e = result["review_effect"]
    if rev_e.get("spearman_r") is not None:
        factors.append({"factor": "리뷰 수", "rank_diff": None,
                        "spearman_r": rev_e["spearman_r"], "strength": rev_e["strength"],
                        "significant": rev_e.get("spearman_p", 1) < 0.05, "p_value": rev_e.get("spearman_p")})

    # 순위 차이 절대값 기준 정렬
    factors.sort(key=lambda x: abs(x.get("rank_diff") or 0), reverse=True)
    result["factor_summary"] = factors

    return result


# ══════════════════════════════════════════════
# 통합 실행 + 콘솔 출력
# ══════════════════════════════════════════════

def run_reverse_engineering(keyword: str, config: AnalysisConfig = None) -> dict:
    """키워드의 수집 데이터로 쿠팡 시스템 역공학 전체 실행."""
    config = config or AnalysisConfig()
    db = CoupangDB(config)
    df = db.get_analysis_dataframe(keyword)

    if df.empty:
        print(f"\n  '{keyword}' 데이터가 없습니다. 먼저 collect 또는 import를 실행하세요.")
        db.close()
        return {}

    print(f"\n{'='*70}")
    print(f"  쿠팡 시스템 역공학 분석")
    print(f"  키워드: {keyword} ({len(df)}개 상품)")
    print(f"{'='*70}")

    print("\n  [1/8] SERP 구조 분석...")
    serp = reverse_serp_structure(df)

    print("  [2/8] ID 체계 분석...")
    ids = reverse_id_system(df)

    print("  [3/8] 순위 결정 심층 분석...")
    ranking = reverse_ranking_deep(df)

    print("  [4/8] sourceType 해독...")
    source = reverse_source_type(df)

    print("  [5/8] 배송 부스트 정량화...")
    delivery = reverse_delivery_boost(df)

    print("  [6/8] 가격 알고리즘 분석...")
    price = reverse_price_algorithm(df)

    print("  [7/8] 시간 패턴 분석...")
    time_patterns = reverse_time_patterns(keyword, db)

    print("  [8/8] 셀러 컨트롤 요소 분석...")
    seller_factors = reverse_seller_factors(df, keyword=keyword, db=db)

    db.close()

    results = {
        "keyword": keyword,
        "total_products": len(df),
        "serp_structure": serp,
        "id_system": ids,
        "ranking_deep": ranking,
        "source_types": source,
        "delivery_boost": delivery,
        "price_algorithm": price,
        "time_patterns": time_patterns,
        "seller_factors": seller_factors,
    }

    _print_reverse_report(results)
    return results


def _print_reverse_report(r: dict):
    """역공학 결과를 콘솔에 출력."""
    SEP = "=" * 70
    LINE = "─" * 70

    # 1. SERP 구조
    serp = r["serp_structure"]
    print(f"\n{LINE}")
    print(f"  [1] SERP 구조 (검색 결과 페이지 규칙)")
    print(f"{LINE}")

    asp = serp.get("ad_slot_pattern", {})
    print(f"  광고 {asp.get('total_ads', 0)}개 / 전체 {asp.get('total_products', 0)}개")
    if asp.get("ad_positions"):
        print(f"  광고 위치: {asp['ad_positions']}")
    if asp.get("section_density"):
        for sec, cnt in asp["section_density"].items():
            if cnt > 0:
                print(f"    {sec}: 광고 {cnt}개")

    air = serp.get("ad_interval_rule", {})
    if air.get("pattern"):
        print(f"  광고 간격: {air['pattern']} (평균 {air.get('avg_interval', 0)}, 표준편차 {air.get('stddev', 0)})")

    div = serp.get("diversity_rules", {})
    if div.get("rule_detected"):
        print(f"  다양성: {div['rule_detected']}")
        print(f"    셀러 다양성: {div.get('unique_sellers', 0)}개 / 비율 {div.get('seller_diversity_ratio', 0)}")

    dp = serp.get("delivery_placement", {})
    if dp:
        print(f"  배송유형별 평균 위치:")
        for dt, info in sorted(dp.items(), key=lambda x: x[1]["avg_position"]):
            print(f"    {dt}: 평균 {info['avg_position']}위 ({info['count']}개, 상위10에 {info['positions_top10']}개)")

    # 2. ID 체계
    ids = r["id_system"]
    print(f"\n{LINE}")
    print(f"  [2] ID 체계 (카탈로그 구조)")
    print(f"{LINE}")

    idc = ids.get("id_counts", {})
    print(f"  productId: {idc.get('unique_product_ids', 0)}개 (카탈로그)")
    print(f"  itemId: {idc.get('unique_item_ids', 0)}개 (옵션/변형)")
    print(f"  vendorItemId: {idc.get('unique_vendor_item_ids', 0)}개 (셀러×상품)")

    rel = ids.get("relationships", {})
    print(f"  1 카탈로그당 평균 {rel.get('items_per_product', 0)} 옵션, {rel.get('vendors_per_product', 0)} 셀러")

    cs = ids.get("catalog_structure", {})
    if cs.get("multi_seller_products", 0) > 0:
        print(f"  멀티셀러 상품: {cs['multi_seller_products']}개 (최대 {cs['max_sellers_per_product']}셀러)")

    if ids.get("multi_seller_products"):
        print(f"  멀티셀러 예시:")
        for mp in ids["multi_seller_products"][:3]:
            prices_str = ", ".join(f"{p:,}원" for p in mp["prices"] if p)
            print(f"    {mp['product_name']} ({mp['seller_count']}셀러, 가격: {prices_str})")

    # 3. 순위 결정 심층
    rk = r["ranking_deep"]
    print(f"\n{LINE}")
    print(f"  [3] 순위 결정 심층 (비선형 + 임계값 + 상호작용)")
    print(f"{LINE}")

    if rk.get("note"):
        print(f"  {rk['note']}")

    mp = rk.get("model_performance", {})
    if mp:
        print(f"  모델: {mp.get('model', '')} (R² = {mp.get('r_squared', 0)}, {mp.get('interpretation', '')})")

    if rk.get("factor_ranking"):
        print(f"\n  팩터 중요도:")
        for f in rk["factor_ranking"]:
            bar = "█" * int(float(f["importance"]) * 50)
            print(f"    {f['factor']:>10}: {f['pct']:>6} {bar}")

    if rk.get("nonlinear_effects"):
        print(f"\n  비선형 효과:")
        for factor, info in rk["nonlinear_effects"].items():
            nl = "비선형" if info["is_nonlinear"] else "선형"
            print(f"    {factor}: {nl} (방향: {info['direction']}, 효과범위: {info['effect_range']})")

    if rk.get("thresholds"):
        print(f"\n  임계값 (급격한 순위 변화 지점):")
        for factor, info in rk["thresholds"].items():
            print(f"    {info['interpretation']}")

    if rk.get("interactions"):
        print(f"\n  상호작용:")
        for pair, info in rk["interactions"].items():
            sig = "강함" if info["significant"] else "약함"
            print(f"    {pair}: {sig} (비율: {info['ratio']})")

    # 4. sourceType
    src = r["source_types"]
    if src.get("types"):
        print(f"\n{LINE}")
        print(f"  [4] sourceType 해독")
        print(f"{LINE}")
        for st, info in src["types"].items():
            interp = src.get("interpretation", {}).get(st, [])
            interp_str = ", ".join(interp) if interp else ""
            print(f"  '{st}': {info['count']}개 ({info['share_pct']}%), "
                  f"광고율 {info['ad_ratio']}%, 로켓 {info['rocket_ratio']}%"
                  f"{' → ' + interp_str if interp_str else ''}")

    # 5. 배송 부스트
    dv = r["delivery_boost"]
    if dv.get("overall"):
        print(f"\n{LINE}")
        print(f"  [5] 배송 유형 부스트")
        print(f"{LINE}")
        ov = dv["overall"]
        if ov.get("rocket_count", 0) == 0 and ov.get("marketplace_count", 0) > 0:
            print(f"  이 키워드는 마켓플레이스 전용 (로켓배송 없음)")
            print(f"  마켓플레이스: 평균 {ov.get('marketplace_avg_rank', 'N/A')}위 ({ov.get('marketplace_count', 0)}개)")
        else:
            print(f"  로켓배송: 평균 {ov.get('rocket_avg_rank', 'N/A')}위 ({ov.get('rocket_count', 0)}개)")
            print(f"  마켓플레이스: 평균 {ov.get('marketplace_avg_rank', 'N/A')}위 ({ov.get('marketplace_count', 0)}개)")
            if ov.get("rank_advantage"):
                print(f"  순위 이점: {ov['rank_advantage']}위")
            if ov.get("boost_interpretation"):
                print(f"  통제 후: {ov['boost_interpretation']}")

            st = dv.get("statistical_test", {})
            if st:
                print(f"  검정: {st.get('conclusion', '')} (p={st.get('p_value', 'N/A')})")

    # 6. 가격 알고리즘
    pr = r["price_algorithm"]
    if pr.get("sweet_spot"):
        print(f"\n{LINE}")
        print(f"  [6] 가격 알고리즘")
        print(f"{LINE}")
        print(f"  최적 가격대: {pr['sweet_spot'].get('interpretation', '')}")

    if pr.get("outlier_penalty"):
        op = pr["outlier_penalty"]
        if op.get("penalty_detected"):
            print(f"  패널티: {op['penalty_detected']}")
        print(f"  정상가 평균순위: {op.get('normal_avg_rank', 'N/A')}위 ({op.get('normal_count', 0)}개)")
        print(f"  고가 평균순위: {op.get('expensive_avg_rank', 'N/A')}위 ({op.get('expensive_count', 0)}개)")
        print(f"  저가 평균순위: {op.get('cheap_avg_rank', 'N/A')}위 ({op.get('cheap_count', 0)}개)")

    if pr.get("discount_effect"):
        de = pr["discount_effect"]
        print(f"  할인 효과: {de.get('conclusion', '')}")

    # 7. 시간 패턴
    tp = r["time_patterns"]
    print(f"\n{LINE}")
    print(f"  [7] 시간 패턴")
    print(f"{LINE}")
    print(f"  스냅샷: {tp.get('snapshot_count', 0)}개")

    if tp.get("note"):
        print(f"  {tp['note']}")

    sa = tp.get("stability_analysis", {})
    if sa.get("always_present_count"):
        print(f"  항상 노출 상품: {sa['always_present_count']}개 / 전체 {sa['ever_present_count']}개")
        print(f"  안정성 비율: {sa.get('stability_ratio', 0)}")

    traits = sa.get("top_stable_traits", {})
    if traits:
        print(f"  상위 안정 상품 특성:")
        print(f"    평균 리뷰: {traits.get('avg_reviews', 0)}개")
        print(f"    평균 가격: {int(traits.get('avg_price', 0)):,}원")
        print(f"    로켓 비율: {traits.get('rocket_pct', 0)}%")
        print(f"    순위 변동: ±{traits.get('avg_rank_volatility', 0)}")

    ep = tp.get("entry_patterns", {})
    if ep.get("total_new_entries"):
        print(f"  신규 진입: {ep['total_new_entries']}개, 평균 {ep.get('avg_entry_rank', 'N/A')}위에서 시작")
        print(f"    Top10 직행: {ep.get('entered_top10_count', 0)}개")

    ri = tp.get("rank_inertia", {})
    if ri.get("interpretation"):
        print(f"  순위 관성: {ri['interpretation']}")

    # 8. 셀러 컨트롤 요소
    sf = r.get("seller_factors", {})
    if sf:
        print(f"\n{LINE}")
        print(f"  [8] 셀러 컨트롤 요소 (등록 시 영향도)")
        print(f"{LINE}")

        if sf.get("note"):
            print(f"  {sf['note']}")
        else:
            # 검색어 효과
            kw_e = sf.get("keyword_effect", {})
            if kw_e.get("has_keyword_avg_rank") is not None:
                print(f"\n  검색어 (상품명 내 키워드):")
                print(f"    포함: 평균 {kw_e['has_keyword_avg_rank']}위 ({kw_e['has_keyword_count']}개)")
                if kw_e.get("no_keyword_avg_rank") is not None:
                    print(f"    미포함: 평균 {kw_e['no_keyword_avg_rank']}위 ({kw_e['no_keyword_count']}개)")
                if kw_e.get("rank_diff") is not None:
                    sig_mark = " ***" if kw_e.get("significant") else ""
                    print(f"    → {abs(kw_e['rank_diff']):.1f}위 차이{sig_mark}")
                pos = kw_e.get("position", {})
                if pos.get("front_avg_rank") is not None:
                    print(f"    위치: 앞쪽 {pos['front_avg_rank']}위 vs 뒤쪽 {pos['back_avg_rank']}위")
                nl = kw_e.get("name_length", {})
                if nl:
                    print(f"    길이: 짧은 {nl['short']['avg_rank']}위 / "
                          f"보통 {nl['medium']['avg_rank']}위 / "
                          f"긴 {nl['long']['avg_rank']}위")

            # 카탈로그 매칭
            cat_e = sf.get("catalog_effect", {})
            if cat_e.get("multi_seller_avg_rank") is not None or cat_e.get("single_seller_avg_rank") is not None:
                print(f"\n  카탈로그 매칭:")
                if cat_e.get("multi_seller_avg_rank") is not None:
                    print(f"    멀티셀러: 평균 {cat_e['multi_seller_avg_rank']}위 ({cat_e['multi_seller_count']}개)")
                if cat_e.get("single_seller_avg_rank") is not None:
                    print(f"    단독: 평균 {cat_e['single_seller_avg_rank']}위 ({cat_e['single_seller_count']}개)")
                if cat_e.get("rank_diff") is not None:
                    sig_mark = " ***" if cat_e.get("significant") else ""
                    direction = "멀티셀러 유리" if cat_e["rank_diff"] > 0 else "단독 유리"
                    print(f"    → {abs(cat_e['rank_diff']):.1f}위 차이 ({direction}){sig_mark}")
                if cat_e.get("multi_avg_reviews") is not None:
                    print(f"    멀티셀러 리뷰 {cat_e['multi_avg_reviews']:.0f}개 vs 단독 {cat_e['single_avg_reviews']:.0f}개")

            # 광고
            ad_e = sf.get("ad_effect", {})
            if ad_e:
                print(f"\n  광고 효과:")
                print(f"    광고 {ad_e.get('ad_count', 0)}개 ({ad_e.get('ad_ratio', 0)}%) / "
                      f"자연 {ad_e.get('organic_count', 0)}개")
                if ad_e.get("avg_exposure_order"):
                    print(f"    광고 평균 위치: {ad_e['avg_exposure_order']}위")
                if ad_e.get("overlap_count", 0) > 0:
                    print(f"    광고+자연 동시: {ad_e['overlap_count']}개")
                    if ad_e.get("with_ad_avg_rank") is not None:
                        sig_mark = " ***" if ad_e.get("significant") else ""
                        print(f"      광고도 하는 상품: 자연순위 {ad_e['with_ad_avg_rank']}위")
                        print(f"      광고 안 하는 상품: 자연순위 {ad_e['without_ad_avg_rank']}위")
                        print(f"      → {abs(ad_e.get('rank_diff', 0)):.1f}위 차이{sig_mark}")
                elif ad_e.get("ad_count", 0) > 0:
                    print(f"    광고+자연 동시 노출 없음 (서로 다른 셀러)")

            # 할인율
            disc_e = sf.get("discount_effect", {})
            if disc_e.get("discounted_avg_rank") is not None:
                print(f"\n  할인율:")
                print(f"    할인 있음: 평균 {disc_e['discounted_avg_rank']}위 ({disc_e['discounted_count']}개)")
                print(f"    할인 없음: 평균 {disc_e['no_discount_avg_rank']}위 ({disc_e['no_discount_count']}개)")
                if disc_e.get("rank_diff") is not None:
                    sig_mark = " ***" if disc_e.get("significant") else ""
                    print(f"    → {abs(disc_e['rank_diff']):.1f}위 차이{sig_mark}")

            # 리뷰
            rev_e = sf.get("review_effect", {})
            if rev_e.get("tiers"):
                print(f"\n  리뷰 수:")
                for tier, info in rev_e["tiers"].items():
                    print(f"    {tier:>6}개: 평균 {info['avg_rank']}위 ({info['count']}개)")
                if rev_e.get("spearman_r") is not None:
                    print(f"    Spearman r={rev_e['spearman_r']}, p={rev_e['spearman_p']}"
                          f" → {rev_e['direction']} (강도: {rev_e['strength']})")

            # 적립금
            cb_e = sf.get("cashback_effect", {})
            if cb_e.get("with_cashback_count", 0) > 0 and cb_e.get("no_cashback_count", 0) > 0:
                print(f"\n  적립금:")
                print(f"    있음: 평균 {cb_e['with_cashback_avg_rank']}위 ({cb_e['with_cashback_count']}개)")
                print(f"    없음: 평균 {cb_e['no_cashback_avg_rank']}위 ({cb_e['no_cashback_count']}개)")
                if cb_e.get("rank_diff") is not None:
                    sig_mark = " ***" if cb_e.get("significant") else ""
                    print(f"    → {abs(cb_e['rank_diff']):.1f}위 차이{sig_mark}")
            elif cb_e.get("with_cashback_count", 0) > 0:
                print(f"\n  적립금: 전 상품 적립금 있음 ({cb_e['with_cashback_count']}개)")
            elif cb_e.get("no_cashback_count", 0) > 0:
                print(f"\n  적립금: 전 상품 적립금 없음 ({cb_e['no_cashback_count']}개)")

            # 가격 포지셔닝
            pp = sf.get("price_positioning", {})
            if pp.get("median_price"):
                print(f"\n  가격 포지셔닝 (중앙값 {pp['median_price']:,.0f}원 기준):")
                if pp.get("cheap_avg_rank") is not None:
                    print(f"    저가 (<80%): 평균 {pp['cheap_avg_rank']}위 ({pp['cheap_count']}개)")
                if pp.get("normal_avg_rank") is not None:
                    print(f"    적정가 (80~120%): 평균 {pp['normal_avg_rank']}위 ({pp['normal_count']}개)")
                if pp.get("expensive_avg_rank") is not None:
                    print(f"    고가 (>120%): 평균 {pp['expensive_avg_rank']}위 ({pp['expensive_count']}개)")

            # 종합 영향도
            summary = sf.get("factor_summary", [])
            if summary:
                print(f"\n  종합 영향도 랭킹 (순위 차이 기준):")
                print(f"  {'─'*60}")
                for i, f in enumerate(summary, 1):
                    sig = "***" if f.get("significant") else "   "
                    p_str = f"p={f['p_value']:.4f}" if f.get("p_value") is not None else ""
                    if f.get("rank_diff") is not None:
                        bar_len = min(int(abs(f["rank_diff"]) / 2), 20)
                        bar = "█" * bar_len
                        print(f"    {i}. {f['factor']:<12} {abs(f['rank_diff']):>5.1f}위 {sig} {bar:<20} {p_str}")
                    elif f.get("spearman_r") is not None:
                        strength = "상관" if abs(f["spearman_r"]) > 0.3 else "약함/무관"
                        print(f"    {i}. {f['factor']:<12} r={f['spearman_r']:.3f} {sig} ({strength}) {p_str}")
                print(f"  {'─'*60}")
                print(f"    (*** = 통계적으로 유의미, p<0.05)")

    print(f"\n{SEP}")
    print(f"  역공학 분석 완료")
    print(f"  데이터가 많을수록 정확합니다. 매일 collect로 스냅샷을 쌓으세요.")
    print(f"{SEP}")
