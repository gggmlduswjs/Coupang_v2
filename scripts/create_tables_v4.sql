-- ============================================================
-- Supabase PostgreSQL 통합 DB v4
-- 23개 테이블 — 쿠팡데이터분석 + Coupong + 쿠팡비즈니스 통합
-- 실행: Supabase SQL Editor에서 전체 복사-붙여넣기
-- ============================================================

-- 기존 테이블 DROP (의존성 역순)
DROP TABLE IF EXISTS analysis_results CASCADE;
DROP TABLE IF EXISTS catalog_matches CASCADE;
DROP TABLE IF EXISTS exposure_logs CASCADE;
DROP TABLE IF EXISTS ad_performances CASCADE;
DROP TABLE IF EXISTS ad_spends CASCADE;
DROP TABLE IF EXISTS return_requests CASCADE;
DROP TABLE IF EXISTS settlement_history CASCADE;
DROP TABLE IF EXISTS revenue_history CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS product_changes CASCADE;
DROP TABLE IF EXISTS sync_log CASCADE;
DROP TABLE IF EXISTS deleted_listings CASCADE;
DROP TABLE IF EXISTS listings CASCADE;
DROP TABLE IF EXISTS master_catalog CASCADE;
DROP TABLE IF EXISTS bundle_items CASCADE;
DROP TABLE IF EXISTS bundle_skus CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS books CASCADE;
DROP TABLE IF EXISTS publishers CASCADE;
DROP TABLE IF EXISTS search_results CASCADE;
DROP TABLE IF EXISTS snapshots CASCADE;
DROP TABLE IF EXISTS keywords CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;

-- 기존 통합 전 테이블도 정리
DROP TABLE IF EXISTS seller_accounts CASCADE;
DROP TABLE IF EXISTS account_products CASCADE;
DROP TABLE IF EXISTS inventory_products CASCADE;
DROP TABLE IF EXISTS inventory_snapshots CASCADE;
DROP TABLE IF EXISTS catalog_matching_progress CASCADE;
DROP TABLE IF EXISTS master_products CASCADE;


-- ============================================================
-- A. 마스터 데이터
-- ============================================================

-- 1. accounts (통합 계정)
CREATE TABLE accounts (
    id              SERIAL PRIMARY KEY,
    account_code    VARCHAR(20)  NOT NULL UNIQUE,
    account_name    VARCHAR(50)  NOT NULL,
    email           VARCHAR(100) DEFAULT '',
    is_active       BOOLEAN      DEFAULT true,
    status          VARCHAR(20)  DEFAULT '활성',
    vendor_id       VARCHAR(20),
    wing_access_key VARCHAR(100),  -- TODO: 운영 시 Supabase Vault 또는 암호화 적용 필요
    wing_secret_key VARCHAR(100),  -- TODO: 운영 시 Supabase Vault 또는 암호화 적용 필요
    wing_api_enabled BOOLEAN     DEFAULT false,
    outbound_shipping_code VARCHAR(50),
    return_center_code     VARCHAR(50),
    memo            TEXT         DEFAULT '',
    created_at      TIMESTAMPTZ  DEFAULT now(),
    updated_at      TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_accounts_name   ON accounts (account_name);
CREATE INDEX ix_accounts_vendor ON accounts (vendor_id);

COMMENT ON TABLE accounts IS '통합 계정 (Coupong accounts + 쿠팡데이터분석 seller_accounts)';


-- 2. publishers (출판사)
CREATE TABLE publishers (
    id                SERIAL PRIMARY KEY,
    name              VARCHAR(100) NOT NULL UNIQUE,
    margin_rate       INTEGER      NOT NULL,
    min_free_shipping INTEGER      NOT NULL,
    supply_rate       FLOAT        NOT NULL,
    is_active         BOOLEAN      DEFAULT true,
    notes             TEXT,
    created_at        TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_publishers_active ON publishers (is_active);

COMMENT ON TABLE publishers IS '출판사 마스터 (매입률/공급률 관리)';


-- ============================================================
-- B. 도서/상품 파이프라인
-- ============================================================

-- 3. books (도서 원본)
CREATE TABLE books (
    id                SERIAL PRIMARY KEY,
    isbn              VARCHAR(13)  NOT NULL UNIQUE,
    title             VARCHAR(500) NOT NULL,
    author            VARCHAR(200),
    publisher_id      INTEGER      REFERENCES publishers(id) ON DELETE SET NULL,
    list_price        INTEGER      NOT NULL,
    year              INTEGER,
    normalized_title  VARCHAR(500),
    normalized_series VARCHAR(200),
    sales_point       INTEGER      DEFAULT 0,
    crawled_at        TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_books_publisher ON books (publisher_id);
CREATE INDEX ix_books_year      ON books (year);
CREATE INDEX ix_books_series    ON books (normalized_series);
CREATE INDEX ix_books_sales     ON books (sales_point);

COMMENT ON TABLE books IS '알라딘 ISBN 기반 도서 메타데이터';


-- 4. products (단권 상품)
CREATE TABLE products (
    id               SERIAL PRIMARY KEY,
    book_id          INTEGER      NOT NULL REFERENCES books(id) ON DELETE CASCADE,
    isbn             VARCHAR(13),
    list_price       INTEGER      NOT NULL,
    sale_price       INTEGER      NOT NULL,
    supply_rate      FLOAT        NOT NULL,
    margin_per_unit  INTEGER      NOT NULL,
    shipping_cost    INTEGER      DEFAULT 2300,
    net_margin       INTEGER      NOT NULL,
    shipping_policy  VARCHAR(20)  NOT NULL,
    publisher_id     INTEGER      REFERENCES publishers(id) ON DELETE SET NULL,
    can_upload_single BOOLEAN     DEFAULT true,
    status           VARCHAR(20)  DEFAULT 'ready',
    exclude_reason   TEXT,
    created_at       TIMESTAMPTZ  DEFAULT now(),
    updated_at       TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_products_book      ON products (book_id);
CREATE INDEX ix_products_isbn      ON products (isbn);
CREATE INDEX ix_products_status    ON products (status);
CREATE INDEX ix_products_shipping  ON products (shipping_policy);
CREATE INDEX ix_products_publisher ON products (publisher_id);

COMMENT ON TABLE products IS '단권 상품 (도서 1권 = 1상품)';


-- 5. bundle_skus (묶음 상품)
CREATE TABLE bundle_skus (
    id                SERIAL PRIMARY KEY,
    bundle_key        VARCHAR(200) NOT NULL UNIQUE,
    bundle_name       VARCHAR(300) NOT NULL,
    publisher_id      INTEGER      NOT NULL REFERENCES publishers(id) ON DELETE CASCADE,
    normalized_series VARCHAR(200) NOT NULL,
    year              INTEGER      NOT NULL,
    book_count        INTEGER      NOT NULL,
    total_list_price  INTEGER      NOT NULL,
    total_sale_price  INTEGER      NOT NULL,
    supply_rate       FLOAT        NOT NULL,
    total_margin      INTEGER      NOT NULL,
    shipping_cost     INTEGER      DEFAULT 2300,
    net_margin        INTEGER      NOT NULL,
    shipping_policy   VARCHAR(20)  DEFAULT 'free',
    created_at        TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_bundles_publisher ON bundle_skus (publisher_id);
CREATE INDEX ix_bundles_series    ON bundle_skus (normalized_series);
CREATE INDEX ix_bundles_year      ON bundle_skus (year);

COMMENT ON TABLE bundle_skus IS '묶음 상품 (시리즈별 세트)';


-- 6. bundle_items (묶음 구성)
CREATE TABLE bundle_items (
    id        SERIAL PRIMARY KEY,
    bundle_id INTEGER     NOT NULL REFERENCES bundle_skus(id) ON DELETE CASCADE,
    book_id   INTEGER     NOT NULL REFERENCES books(id) ON DELETE CASCADE,
    isbn      VARCHAR(13) NOT NULL,
    UNIQUE (bundle_id, book_id)
);

CREATE INDEX ix_bundle_item_book ON bundle_items (book_id);

COMMENT ON TABLE bundle_items IS '묶음 상품 구성 도서 목록';


-- ============================================================
-- C. 마스터 카탈로그
-- ============================================================

-- 7. master_catalog (마스터 카탈로그)
CREATE TABLE master_catalog (
    id                  SERIAL PRIMARY KEY,
    isbn                VARCHAR(50)  DEFAULT '',
    canonical_name      VARCHAR(500) NOT NULL,
    coupang_catalog_id  VARCHAR(50)  NOT NULL UNIQUE,
    category            VARCHAR(200) DEFAULT '',
    publisher           VARCHAR(200) DEFAULT '',
    brand               VARCHAR(200) DEFAULT '',
    base_price          INTEGER,
    product_type        VARCHAR(20)  DEFAULT '단품',
    set_composition     JSONB        DEFAULT '[]'::jsonb,
    adult_only          VARCHAR(5)   DEFAULT '',
    model_number        VARCHAR(100) DEFAULT '',
    publisher_id        INTEGER      REFERENCES publishers(id) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ  DEFAULT now(),
    updated_at          TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_master_isbn      ON master_catalog (isbn);
CREATE INDEX ix_master_type      ON master_catalog (product_type);
CREATE INDEX ix_master_publisher ON master_catalog (publisher_id);

COMMENT ON TABLE master_catalog IS '쿠팡 노출상품ID 기준 크로스-계정 마스터 카탈로그';


-- 8. sync_log (계정 간 동기화 이력)
CREATE TABLE sync_log (
    id                 SERIAL PRIMARY KEY,
    master_catalog_id  INTEGER REFERENCES master_catalog(id) ON DELETE CASCADE,
    source_account_id  INTEGER REFERENCES accounts(id) ON DELETE SET NULL,
    target_account_id  INTEGER REFERENCES accounts(id) ON DELETE SET NULL,
    action             VARCHAR(100) NOT NULL,
    before_value       TEXT         DEFAULT '',
    after_value        TEXT         DEFAULT '',
    created_at         TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_sync_master ON sync_log (master_catalog_id);

COMMENT ON TABLE sync_log IS '계정 간 동기화 이력 (수정/이미지복제 등)';


-- ============================================================
-- D. 상품 등록/관리
-- ============================================================

-- 9. listings (통합 계정별 등록 상품) ★핵심★
CREATE TABLE listings (
    id                          SERIAL PRIMARY KEY,
    account_id                  INTEGER      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    coupang_product_id          BIGINT       NOT NULL,
    vendor_item_id              BIGINT,
    product_name                VARCHAR(500),
    coupang_status              VARCHAR(20)  DEFAULT 'pending',
    original_price              INTEGER      DEFAULT 0,
    sale_price                  INTEGER      DEFAULT 0,
    supply_price                INTEGER,
    stock_quantity              INTEGER      DEFAULT 0,
    display_category_code       VARCHAR(20),
    delivery_charge_type        VARCHAR(20),
    delivery_charge             INTEGER,
    free_ship_over_amount       INTEGER,
    return_charge               INTEGER,
    brand                       VARCHAR(200),
    isbn                        TEXT,
    product_id                  INTEGER      REFERENCES products(id) ON DELETE SET NULL,
    bundle_id                   INTEGER      REFERENCES bundle_skus(id) ON DELETE SET NULL,
    master_catalog_id           INTEGER      REFERENCES master_catalog(id) ON DELETE SET NULL,
    raw_json                    TEXT,
    detail_synced_at            TIMESTAMPTZ,
    synced_at                   TIMESTAMPTZ,
    -- 쿠팡데이터분석 account_products 전용 필드
    approval_status             VARCHAR(20)  DEFAULT '',
    search_tags                 TEXT         DEFAULT '',
    barcode                     VARCHAR(50)  DEFAULT '',
    model_number                VARCHAR(100) DEFAULT '',
    option_name                 VARCHAR(200) DEFAULT '',
    sold_quantity               INTEGER      DEFAULT 0,
    seller_product_code         VARCHAR(100) DEFAULT '',
    coupang_display_product_id  VARCHAR(50)  DEFAULT '',
    product_status              VARCHAR(20)  DEFAULT '',
    adult_only                  VARCHAR(5)   DEFAULT '',
    publisher_id                INTEGER      REFERENCES publishers(id) ON DELETE SET NULL,
    purchase_options            JSONB        DEFAULT '[]'::jsonb,
    search_options              JSONB        DEFAULT '[]'::jsonb,
    images                      JSONB        DEFAULT '[]'::jsonb,
    created_at                  TIMESTAMPTZ  DEFAULT now(),
    updated_at                  TIMESTAMPTZ  DEFAULT now(),
    UNIQUE (account_id, coupang_product_id)
);

CREATE INDEX ix_listing_account_status      ON listings (account_id, coupang_status);
CREATE INDEX ix_listing_account_vendor_item ON listings (account_id, vendor_item_id);
CREATE INDEX ix_listing_isbn                ON listings (isbn);
CREATE INDEX ix_listing_product_id          ON listings (product_id);
CREATE INDEX ix_listing_bundle_id           ON listings (bundle_id);
CREATE INDEX ix_listing_master_catalog_id   ON listings (master_catalog_id);
CREATE INDEX ix_listing_synced_at           ON listings (synced_at);
CREATE INDEX ix_listing_barcode             ON listings (barcode);
CREATE INDEX ix_listing_publisher           ON listings (publisher_id);

COMMENT ON TABLE listings IS '통합 계정별 등록 상품 (Coupong listings + 쿠팡데이터분석 account_products)';


-- 10. deleted_listings (삭제 아카이브)
CREATE TABLE deleted_listings (
    id                     SERIAL PRIMARY KEY,
    account_id             INTEGER      NOT NULL,
    coupang_product_id     BIGINT       NOT NULL,
    vendor_item_id         BIGINT,
    product_name           VARCHAR(500),
    coupang_status         VARCHAR(20),
    original_price         INTEGER      DEFAULT 0,
    sale_price             INTEGER      DEFAULT 0,
    supply_price           INTEGER,
    stock_quantity         INTEGER      DEFAULT 0,
    display_category_code  VARCHAR(20),
    delivery_charge_type   VARCHAR(20),
    delivery_charge        INTEGER,
    free_ship_over_amount  INTEGER,
    return_charge          INTEGER,
    brand                  VARCHAR(200),
    isbn                   TEXT,
    product_id             INTEGER,
    bundle_id              INTEGER,
    raw_json               TEXT,
    detail_synced_at       TIMESTAMPTZ,
    synced_at              TIMESTAMPTZ,
    original_created_at    TIMESTAMPTZ,
    deleted_reason         VARCHAR(200) DEFAULT '쿠팡 삭제 확인',
    deleted_at             TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_deleted_account ON deleted_listings (account_id);

COMMENT ON TABLE deleted_listings IS '삭제된 listing 아카이브 (FK 없음)';


-- 11. product_changes (API 수정 이력)
CREATE TABLE product_changes (
    id                 SERIAL PRIMARY KEY,
    account_id         INTEGER     NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    seller_product_id  VARCHAR(50) NOT NULL,
    action             VARCHAR(50) NOT NULL,
    field              VARCHAR(100) DEFAULT '',
    before_value       TEXT         DEFAULT '',
    after_value        TEXT         DEFAULT '',
    result_code        VARCHAR(50)  DEFAULT '',
    changed_at         TIMESTAMPTZ  NOT NULL
);

CREATE INDEX ix_changes_account ON product_changes (account_id);
CREATE INDEX ix_changes_spid    ON product_changes (seller_product_id);
CREATE INDEX ix_changes_changed ON product_changes (changed_at);

COMMENT ON TABLE product_changes IS 'API 상품 수정 이력 (등록/수정/삭제/상태변경)';


-- ============================================================
-- E. 주문/매출/정산
-- ============================================================

-- 12. orders (발주서)
CREATE TABLE orders (
    id                    SERIAL PRIMARY KEY,
    account_id            INTEGER      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    shipment_box_id       BIGINT       NOT NULL,
    order_id              BIGINT       NOT NULL,
    vendor_item_id        BIGINT,
    status                VARCHAR(30),
    ordered_at            TIMESTAMPTZ,
    paid_at               TIMESTAMPTZ,
    orderer_name          VARCHAR(100),
    receiver_name         VARCHAR(100),
    receiver_addr         VARCHAR(500),
    receiver_post_code    VARCHAR(10),
    product_id            BIGINT,
    seller_product_id     BIGINT,
    seller_product_name   VARCHAR(500),
    vendor_item_name      VARCHAR(500),
    shipping_count        INTEGER      DEFAULT 0,
    cancel_count          INTEGER      DEFAULT 0,
    hold_count_for_cancel INTEGER      DEFAULT 0,
    sales_price           INTEGER      DEFAULT 0,
    order_price           INTEGER      DEFAULT 0,
    discount_price        INTEGER      DEFAULT 0,
    shipping_price        INTEGER      DEFAULT 0,
    delivery_company_name VARCHAR(50),
    invoice_number        VARCHAR(50),
    shipment_type         VARCHAR(50),
    delivered_date        TIMESTAMPTZ,
    confirm_date          TIMESTAMPTZ,
    refer                 VARCHAR(50),
    canceled              BOOLEAN      DEFAULT false,
    listing_id            INTEGER      REFERENCES listings(id) ON DELETE SET NULL,
    raw_json              TEXT,
    created_at            TIMESTAMPTZ  DEFAULT now(),
    updated_at            TIMESTAMPTZ  DEFAULT now(),
    UNIQUE (account_id, shipment_box_id, vendor_item_id)
);

CREATE INDEX ix_order_account_date    ON orders (account_id, ordered_at);
CREATE INDEX ix_order_account_status  ON orders (account_id, status);
CREATE INDEX ix_order_order_id        ON orders (order_id);
CREATE INDEX ix_order_account_listing ON orders (account_id, listing_id);

COMMENT ON TABLE orders IS '발주서 (WING API shipmentBoxId 기준)';


-- 13. revenue_history (매출 내역)
CREATE TABLE revenue_history (
    id                      SERIAL PRIMARY KEY,
    account_id              INTEGER     NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    order_id                BIGINT      NOT NULL,
    sale_type               VARCHAR(50) NOT NULL,
    sale_date               DATE        NOT NULL,
    recognition_date        DATE        NOT NULL,
    settlement_date         DATE,
    product_id              BIGINT,
    product_name            VARCHAR(500),
    vendor_item_id          BIGINT,
    vendor_item_name        VARCHAR(500),
    sale_price              INTEGER     DEFAULT 0,
    quantity                INTEGER     DEFAULT 0,
    coupang_discount        INTEGER     DEFAULT 0,
    sale_amount             INTEGER     DEFAULT 0,
    seller_discount         INTEGER     DEFAULT 0,
    service_fee             INTEGER     DEFAULT 0,
    service_fee_vat         INTEGER     DEFAULT 0,
    service_fee_ratio       FLOAT,
    settlement_amount       INTEGER     DEFAULT 0,
    delivery_fee_amount     INTEGER     DEFAULT 0,
    delivery_fee_settlement INTEGER     DEFAULT 0,
    listing_id              INTEGER     REFERENCES listings(id) ON DELETE SET NULL,
    created_at              TIMESTAMPTZ DEFAULT now(),
    UNIQUE (account_id, order_id, vendor_item_id)
);

CREATE INDEX ix_rev_account_date ON revenue_history (account_id, recognition_date);
CREATE INDEX ix_rev_recognition  ON revenue_history (recognition_date);
CREATE INDEX ix_rev_listing      ON revenue_history (listing_id);
CREATE INDEX ix_rev_sale_type    ON revenue_history (sale_type);
CREATE INDEX ix_rev_sale_date    ON revenue_history (sale_date);

COMMENT ON TABLE revenue_history IS '매출 내역 (SALE/REFUND)';


-- 14. settlement_history (월간 정산)
CREATE TABLE settlement_history (
    id                       SERIAL PRIMARY KEY,
    account_id               INTEGER     NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    year_month               VARCHAR(7)  NOT NULL,
    settlement_type          VARCHAR(20),
    settlement_date          DATE,
    settlement_status        VARCHAR(20),
    revenue_date_from        DATE,
    revenue_date_to          DATE,
    total_sale               INTEGER     DEFAULT 0,
    service_fee              INTEGER     DEFAULT 0,
    settlement_target_amount INTEGER     DEFAULT 0,
    settlement_amount        INTEGER     DEFAULT 0,
    last_amount              INTEGER     DEFAULT 0,
    pending_released_amount  INTEGER     DEFAULT 0,
    seller_discount_coupon   INTEGER     DEFAULT 0,
    downloadable_coupon      INTEGER     DEFAULT 0,
    seller_service_fee       INTEGER     DEFAULT 0,
    courantee_fee            INTEGER     DEFAULT 0,
    deduction_amount         INTEGER     DEFAULT 0,
    debt_of_last_week        INTEGER     DEFAULT 0,
    final_amount             INTEGER     DEFAULT 0,
    bank_name                VARCHAR(50),
    bank_account             VARCHAR(50),
    raw_json                 TEXT,
    created_at               TIMESTAMPTZ DEFAULT now(),
    UNIQUE (account_id, year_month, settlement_type, settlement_date)
);

CREATE INDEX ix_settle_account_month ON settlement_history (account_id, year_month);
CREATE INDEX ix_settle_month         ON settlement_history (year_month);

COMMENT ON TABLE settlement_history IS '월간/주간 정산 내역';


-- ============================================================
-- F. 반품/광고
-- ============================================================

-- 15. return_requests (반품/취소)
CREATE TABLE return_requests (
    id                         SERIAL PRIMARY KEY,
    account_id                 INTEGER      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    receipt_id                 BIGINT       NOT NULL,
    order_id                   BIGINT,
    payment_id                 BIGINT,
    receipt_type               VARCHAR(50),
    receipt_status             VARCHAR(40),
    created_at_api             TIMESTAMPTZ,
    modified_at_api            TIMESTAMPTZ,
    requester_name             VARCHAR(100),
    requester_phone            VARCHAR(50),
    requester_address          VARCHAR(500),
    requester_address_detail   VARCHAR(200),
    requester_zip_code         VARCHAR(10),
    cancel_reason_category1    VARCHAR(100),
    cancel_reason_category2    VARCHAR(100),
    cancel_reason              TEXT,
    cancel_count_sum           INTEGER,
    return_delivery_id         BIGINT,
    return_delivery_type       VARCHAR(50),
    release_stop_status        VARCHAR(30),
    fault_by_type              VARCHAR(50),
    pre_refund                 BOOLEAN,
    complete_confirm_type      VARCHAR(30),
    complete_confirm_date      TIMESTAMPTZ,
    reason_code                VARCHAR(50),
    reason_code_text           VARCHAR(200),
    return_shipping_charge     INTEGER,
    enclose_price              INTEGER,
    return_items_json          TEXT,
    return_delivery_json       TEXT,
    raw_json                   TEXT,
    listing_id                 INTEGER      REFERENCES listings(id) ON DELETE SET NULL,
    created_at                 TIMESTAMPTZ  DEFAULT now(),
    updated_at                 TIMESTAMPTZ  DEFAULT now(),
    UNIQUE (account_id, receipt_id)
);

CREATE INDEX ix_return_account_created ON return_requests (account_id, created_at_api);
CREATE INDEX ix_return_account_status  ON return_requests (account_id, receipt_status);
CREATE INDEX ix_return_order_id        ON return_requests (order_id);
CREATE INDEX ix_return_listing_id      ON return_requests (listing_id);
CREATE INDEX ix_return_receipt_type    ON return_requests (receipt_type);

COMMENT ON TABLE return_requests IS '반품/취소 접수 내역';


-- 16. ad_performances (광고 성과)
CREATE TABLE ad_performances (
    id                  SERIAL PRIMARY KEY,
    account_id          INTEGER      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    ad_date             DATE         NOT NULL,
    campaign_id         VARCHAR(50)  DEFAULT '',
    campaign_name       VARCHAR(200) DEFAULT '',
    ad_group_name       VARCHAR(200) DEFAULT '',
    coupang_product_id  BIGINT       NOT NULL DEFAULT 0,
    product_name        VARCHAR(500) DEFAULT '',
    listing_id          INTEGER      REFERENCES listings(id) ON DELETE SET NULL,
    keyword             VARCHAR(200) DEFAULT '',
    match_type          VARCHAR(20)  DEFAULT '',
    impressions         INTEGER      DEFAULT 0,
    clicks              INTEGER      DEFAULT 0,
    ctr                 FLOAT        DEFAULT 0.0,
    avg_cpc             INTEGER      DEFAULT 0,
    ad_spend            INTEGER      DEFAULT 0,
    direct_orders       INTEGER      DEFAULT 0,
    direct_revenue      INTEGER      DEFAULT 0,
    indirect_orders     INTEGER      DEFAULT 0,
    indirect_revenue    INTEGER      DEFAULT 0,
    total_orders        INTEGER      DEFAULT 0,
    total_revenue       INTEGER      DEFAULT 0,
    roas                FLOAT        DEFAULT 0.0,
    total_quantity      INTEGER      DEFAULT 0,
    direct_quantity     INTEGER      DEFAULT 0,
    indirect_quantity   INTEGER      DEFAULT 0,
    bid_type            VARCHAR(30)  DEFAULT '',
    sales_method        VARCHAR(20)  DEFAULT '',
    ad_type             VARCHAR(50)  DEFAULT '',
    option_id           VARCHAR(50)  DEFAULT '',
    ad_name             VARCHAR(200) DEFAULT '',
    placement           VARCHAR(100) DEFAULT '',
    creative_id         VARCHAR(50)  DEFAULT '',
    category            VARCHAR(200) DEFAULT '',
    report_type         VARCHAR(20)  DEFAULT 'campaign',
    created_at          TIMESTAMPTZ  DEFAULT now(),
    UNIQUE (account_id, ad_date, campaign_id, ad_group_name, coupang_product_id, keyword, report_type)
);

CREATE INDEX ix_adperf_account_date         ON ad_performances (account_id, ad_date);
CREATE INDEX ix_adperf_listing              ON ad_performances (listing_id);
CREATE INDEX ix_adperf_product              ON ad_performances (coupang_product_id);
CREATE INDEX ix_adperf_account_date_listing ON ad_performances (account_id, ad_date, listing_id);

COMMENT ON TABLE ad_performances IS '광고 성과 리포트 (캠페인/키워드/상품/브랜드/디스플레이)';


-- 17. ad_spends (광고비 정산)
CREATE TABLE ad_spends (
    id                SERIAL PRIMARY KEY,
    account_id        INTEGER     NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    ad_date           DATE        NOT NULL,
    campaign_id       VARCHAR(50) NOT NULL,
    campaign_name     VARCHAR(200),
    ad_type           VARCHAR(20),
    ad_objective      VARCHAR(50),
    daily_budget      INTEGER     DEFAULT 0,
    spent_amount      INTEGER     DEFAULT 0,
    adjustment        INTEGER     DEFAULT 0,
    spent_after_adjust INTEGER    DEFAULT 0,
    over_spend        INTEGER     DEFAULT 0,
    billable_cost     INTEGER     DEFAULT 0,
    vat_amount        INTEGER     DEFAULT 0,
    total_charge      INTEGER     DEFAULT 0,
    created_at        TIMESTAMPTZ DEFAULT now(),
    UNIQUE (account_id, ad_date, campaign_id)
);

CREATE INDEX ix_ad_account_date ON ad_spends (account_id, ad_date);
CREATE INDEX ix_ad_date         ON ad_spends (ad_date);

COMMENT ON TABLE ad_spends IS '광고비 정산 (캠페인별 일별)';


-- ============================================================
-- G. 검색 분석
-- ============================================================

-- 18. keywords (검색 키워드)
CREATE TABLE keywords (
    id         SERIAL PRIMARY KEY,
    keyword    VARCHAR(200) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ  DEFAULT now()
);

COMMENT ON TABLE keywords IS '검색 분석 대상 키워드';


-- 19. snapshots (수집 스냅샷)
CREATE TABLE snapshots (
    id             SERIAL PRIMARY KEY,
    keyword_id     INTEGER     NOT NULL REFERENCES keywords(id) ON DELETE CASCADE,
    collected_at   TIMESTAMPTZ DEFAULT now(),
    source         VARCHAR(50),
    page_count     INTEGER     DEFAULT 0,
    total_products INTEGER     DEFAULT 0
);

CREATE INDEX ix_snapshot_keyword   ON snapshots (keyword_id);
CREATE INDEX ix_snapshot_collected ON snapshots (collected_at);

COMMENT ON TABLE snapshots IS '키워드별 검색결과 수집 스냅샷';


-- 20. search_results (검색 결과)
CREATE TABLE search_results (
    id                 SERIAL PRIMARY KEY,
    snapshot_id        INTEGER      NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    keyword_id         INTEGER      NOT NULL REFERENCES keywords(id) ON DELETE CASCADE,
    exposure_order     INTEGER,
    vendor_item_id     VARCHAR(50),
    ad_type            VARCHAR(20),
    organic_rank       INTEGER,
    product_name       VARCHAR(500),
    original_price     INTEGER      DEFAULT 0,
    discount_rate      FLOAT        DEFAULT 0.0,
    sale_price         INTEGER      DEFAULT 0,
    rating             FLOAT        DEFAULT 0.0,
    review_count       INTEGER      DEFAULT 0,
    url                VARCHAR(500),
    product_id_coupang VARCHAR(50),
    item_id            VARCHAR(50),
    source_type        VARCHAR(50),
    delivery_type      VARCHAR(50),
    arrival_date       VARCHAR(50),
    free_shipping      BOOLEAN      DEFAULT false,
    cashback           VARCHAR(50),
    keyword_in_name    BOOLEAN      DEFAULT false,
    keyword_position   INTEGER,
    category           VARCHAR(200),
    image_count        INTEGER      DEFAULT 0,
    sku                VARCHAR(100),
    created_at         TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_sr_snapshot    ON search_results (snapshot_id);
CREATE INDEX ix_sr_keyword     ON search_results (keyword_id);
CREATE INDEX ix_sr_vendor_item ON search_results (vendor_item_id);
CREATE INDEX ix_sr_product_id  ON search_results (product_id_coupang);
CREATE INDEX ix_sr_organic_rank ON search_results (organic_rank);

COMMENT ON TABLE search_results IS '검색 결과 상품 데이터 (구 products 테이블)';


-- ============================================================
-- H. 모니터링/매칭
-- ============================================================

-- 21. exposure_logs (노출 모니터링)
CREATE TABLE exposure_logs (
    id             SERIAL PRIMARY KEY,
    listing_id     INTEGER      NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
    account_id     INTEGER      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    keyword        VARCHAR(200) NOT NULL,
    found          BOOLEAN      DEFAULT false,
    exposure_rank  INTEGER,
    page           INTEGER,
    matched_by     VARCHAR(50)  DEFAULT '',
    checked_at     TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_exposure_account_keyword ON exposure_logs (account_id, keyword);
CREATE INDEX ix_exposure_checked         ON exposure_logs (checked_at);
CREATE INDEX ix_exposure_listing         ON exposure_logs (listing_id);

COMMENT ON TABLE exposure_logs IS '노출 모니터링 (listing_id 기준, 구 inventory_product_id)';


-- 22. catalog_matches (카탈로그 매칭)
CREATE TABLE catalog_matches (
    id                       SERIAL PRIMARY KEY,
    listing_id               INTEGER      NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
    account_id               INTEGER      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    candidate_product_id     VARCHAR(50)  DEFAULT '',
    candidate_vendor_item_id VARCHAR(50)  DEFAULT '',
    candidate_name           VARCHAR(500) DEFAULT '',
    candidate_price          INTEGER,
    candidate_review_count   INTEGER,
    candidate_rating         FLOAT,
    candidate_url            VARCHAR(500) DEFAULT '',
    candidate_category       VARCHAR(200) DEFAULT '',
    name_score               FLOAT        DEFAULT 0,
    price_score              FLOAT        DEFAULT 0,
    category_score           FLOAT        DEFAULT 0,
    review_bonus             FLOAT        DEFAULT 0,
    total_score              FLOAT        DEFAULT 0,
    confidence               VARCHAR(20)  DEFAULT '낮음',
    rank                     INTEGER      DEFAULT 0,
    status                   VARCHAR(20)  DEFAULT '대기',
    matched_at               TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX ix_catalog_listing ON catalog_matches (listing_id);
CREATE INDEX ix_catalog_account ON catalog_matches (account_id);
CREATE INDEX ix_catalog_score   ON catalog_matches (total_score DESC);
CREATE INDEX ix_catalog_status  ON catalog_matches (status);

COMMENT ON TABLE catalog_matches IS '카탈로그 매칭 후보 (listing_id 기준, 구 inventory_product_id)';


-- 23. analysis_results (판매 분석)
CREATE TABLE analysis_results (
    id              SERIAL PRIMARY KEY,
    listing_id      INTEGER NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
    analysis_date   DATE    NOT NULL,
    period_days     INTEGER DEFAULT 7,
    total_orders    INTEGER,
    conversion_rate FLOAT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX ix_analysis_listing ON analysis_results (listing_id);
CREATE INDEX ix_analysis_date    ON analysis_results (analysis_date);

COMMENT ON TABLE analysis_results IS '상품별 판매 분석 결과';


-- ============================================================
-- updated_at 자동 갱신 트리거
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_master_catalog_updated_at
    BEFORE UPDATE ON master_catalog
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_listings_updated_at
    BEFORE UPDATE ON listings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_return_requests_updated_at
    BEFORE UPDATE ON return_requests
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ============================================================
-- 출판사별 분석 VIEW
-- ============================================================

-- 출판사별 도서 현황
CREATE OR REPLACE VIEW v_publisher_books AS
SELECT
    p.id              AS publisher_id,
    p.name            AS publisher_name,
    p.margin_rate,
    p.supply_rate,
    p.is_active,
    COUNT(b.id)       AS book_count,
    COUNT(pr.id)      AS product_count,
    AVG(pr.net_margin)::INTEGER AS avg_net_margin,
    SUM(CASE WHEN pr.shipping_policy = 'free' THEN 1 ELSE 0 END) AS free_shipping_count,
    SUM(CASE WHEN pr.status = 'ready' THEN 1 ELSE 0 END) AS ready_count
FROM publishers p
LEFT JOIN books b ON p.id = b.publisher_id
LEFT JOIN products pr ON b.id = pr.book_id
GROUP BY p.id, p.name, p.margin_rate, p.supply_rate, p.is_active;

-- 출판사별 등록 상품 현황
CREATE OR REPLACE VIEW v_publisher_listings AS
SELECT
    p.id              AS publisher_id,
    p.name            AS publisher_name,
    a.id              AS account_id,
    a.account_code,
    COUNT(l.id)       AS listing_count,
    SUM(CASE WHEN l.coupang_status = 'APPROVED' THEN 1 ELSE 0 END) AS approved_count,
    SUM(l.sale_price) AS total_sale_value,
    AVG(l.sale_price)::INTEGER AS avg_sale_price
FROM publishers p
JOIN listings l ON p.id = l.publisher_id
JOIN accounts a ON l.account_id = a.id
GROUP BY p.id, p.name, a.id, a.account_code;

-- 출판사별 매출
CREATE OR REPLACE VIEW v_publisher_revenue AS
SELECT
    p.id              AS publisher_id,
    p.name            AS publisher_name,
    a.account_code,
    rh.recognition_date,
    COUNT(rh.id)      AS order_count,
    SUM(rh.sale_amount)   AS total_revenue,
    SUM(rh.settlement_amount) AS total_settlement,
    SUM(rh.service_fee)   AS total_service_fee
FROM publishers p
JOIN listings l ON p.id = l.publisher_id
JOIN accounts a ON l.account_id = a.id
JOIN revenue_history rh ON l.id = rh.listing_id
GROUP BY p.id, p.name, a.account_code, rh.recognition_date;

-- 출판사별 묶음 현황
CREATE OR REPLACE VIEW v_publisher_bundles AS
SELECT
    p.id              AS publisher_id,
    p.name            AS publisher_name,
    COUNT(bs.id)      AS bundle_count,
    SUM(bs.book_count) AS total_books_in_bundles,
    AVG(bs.net_margin)::INTEGER AS avg_bundle_margin,
    SUM(bs.total_sale_price) AS total_bundle_value
FROM publishers p
JOIN bundle_skus bs ON p.id = bs.publisher_id
GROUP BY p.id, p.name;


-- ============================================================
-- RLS (Row Level Security) - 기본 비활성
-- 필요 시 활성화
-- ============================================================

-- ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE listings ENABLE ROW LEVEL SECURITY;


-- ============================================================
-- 완료
-- ============================================================
-- 총 23개 테이블 + 4개 VIEW 생성 완료
-- 테이블: accounts, publishers, books, products, bundle_skus, bundle_items,
--   master_catalog, sync_log, listings, deleted_listings, product_changes,
--   orders, revenue_history, settlement_history, return_requests,
--   ad_performances, ad_spends, keywords, snapshots, search_results,
--   exposure_logs, catalog_matches, analysis_results
-- VIEW: v_publisher_books, v_publisher_listings, v_publisher_revenue, v_publisher_bundles
