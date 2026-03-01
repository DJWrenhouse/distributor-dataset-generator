-- =======================================
-- SYNTHETIC DISTRIBUTOR DATASET SCHEMA
-- CORE TABLES
-- =======================================

CREATE TABLE vendors (
    vendor_id        BIGSERIAL PRIMARY KEY,
    vendor_name      VARCHAR(200) NOT NULL,
    vendor_code      VARCHAR(50)
);

CREATE TABLE distribution_centers (
    dc_id                    BIGSERIAL PRIMARY KEY,
    dc_name                  VARCHAR(200) NOT NULL,
    address_line1            VARCHAR(200),
    city                     VARCHAR(100),
    state                    CHAR(2),
    zip                      VARCHAR(10),
    region                   VARCHAR(50),
    time_zone                VARCHAR(50),
    capacity_units           INTEGER
);

CREATE TABLE items (
    item_id                  BIGSERIAL PRIMARY KEY,
    item_sku                 VARCHAR(100) NOT NULL UNIQUE,
    item_description         VARCHAR(500),
    category                 VARCHAR(100),
    subcategory              VARCHAR(100),
    vendor_id                BIGINT NOT NULL REFERENCES vendors(vendor_id),
    lead_time_days           INTEGER,
    length_in                NUMERIC(10,2),
    width_in                 NUMERIC(10,2),
    height_in                NUMERIC(10,2),
    weight_lb                NUMERIC(10,3),
    cube_ft                  NUMERIC(12,4),
    density_lb_per_cuft      NUMERIC(12,4),
    dimensional_weight_lb    NUMERIC(12,4),
    stackability_flag        BOOLEAN,
    hazmat_flag              BOOLEAN,
    nmfc_class               VARCHAR(20)
);

CREATE TABLE customers (
    customer_id             BIGSERIAL PRIMARY KEY,
    master_account_number   VARCHAR(100),
    sub_account_number      VARCHAR(100),
    customer_name           VARCHAR(200) NOT NULL,
    segment                 VARCHAR(50),
    address_line1           VARCHAR(200),
    city                    VARCHAR(100),
    state                   CHAR(2),
    zip                     VARCHAR(10),
    region                  VARCHAR(50),
    credit_limit            NUMERIC(12,2),
    discount_percentage     NUMERIC(5,2),
    account_open_date       DATE
);

CREATE TABLE item_costs (
    item_cost_id    BIGSERIAL PRIMARY KEY,
    item_id         BIGINT NOT NULL REFERENCES items(item_id),
    effective_date  DATE NOT NULL,
    unit_cost       NUMERIC(12,4),
    unit_price      NUMERIC(12,4),
    shipping_cost_per_unit NUMERIC(12,4),
    freight_cost    NUMERIC(12,4),
    duty_rate       NUMERIC(12,4),
    landed_cost     NUMERIC(12,4),
    replacement_cost NUMERIC(12,4),
    map_price       NUMERIC(12,4)
);

CREATE TABLE vendor_performance (
    vendor_id              BIGINT PRIMARY KEY REFERENCES vendors(vendor_id),
    on_time_delivery_rate  NUMERIC(5,2),
    avg_days_late          NUMERIC(6,2),
    defect_rate            NUMERIC(5,2),
    fill_rate              NUMERIC(5,2),
    last_audit_date        DATE
);

CREATE TABLE vendor_item_details (
    vendor_item_id     BIGSERIAL PRIMARY KEY,
    vendor_id          BIGINT NOT NULL REFERENCES vendors(vendor_id),
    item_id            BIGINT NOT NULL REFERENCES items(item_id),
    vendor_item_number VARCHAR(100),
    moq                INTEGER,
    order_multiple     INTEGER,
    vendor_rank        INTEGER,
    UNIQUE (vendor_id, item_id)
);

CREATE TABLE item_forecast (
    forecast_id          BIGSERIAL PRIMARY KEY,
    item_id              BIGINT NOT NULL REFERENCES items(item_id),
    forecast_month       DATE NOT NULL,
    forecast_quantity    INTEGER,
    abc_class            CHAR(1),
    xyz_class            CHAR(1),
    seasonality_index    NUMERIC(6,3),
    lifecycle_stage      VARCHAR(20),
    forecast_error_mape  NUMERIC(6,2),
    forecast_error_mad   NUMERIC(12,4),
    UNIQUE (item_id, forecast_month)
);

CREATE TABLE inventory_monthly_snapshot (
    snapshot_id          BIGSERIAL PRIMARY KEY,
    dc_id                BIGINT NOT NULL REFERENCES distribution_centers(dc_id),
    item_id              BIGINT NOT NULL REFERENCES items(item_id),
    snapshot_month       DATE NOT NULL,
    quantity_on_hand     INTEGER NOT NULL,
    quantity_allocated   INTEGER DEFAULT 0,
    quantity_available   INTEGER,
    days_on_hand         INTEGER,
    excess_quantity      INTEGER,
    obsolete_quantity    INTEGER,
    aging_0_30           INTEGER,
    aging_31_60          INTEGER,
    aging_61_90          INTEGER,
    aging_90_plus        INTEGER,
    cycle_count_accuracy NUMERIC(5,2)
);

CREATE TABLE inventory_adjustments (
    adjustment_id        BIGSERIAL PRIMARY KEY,
    dc_id                BIGINT NOT NULL REFERENCES distribution_centers(dc_id),
    item_id              BIGINT NOT NULL REFERENCES items(item_id),
    adjustment_type      VARCHAR(50) NOT NULL,
    quantity             INTEGER NOT NULL,
    reason_code          VARCHAR(50),
    user_id              VARCHAR(100),
    adjustment_timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE inventory (
    dc_id               BIGINT NOT NULL REFERENCES distribution_centers(dc_id),
    item_id             BIGINT NOT NULL REFERENCES items(item_id),
    avg_cost            NUMERIC(12,4),
    last_cost           NUMERIC(12,4),
    std_cost            NUMERIC(12,4),
    qty_on_hand         INTEGER,
    qty_on_order        INTEGER,
    stocking_location   VARCHAR(100),
    vendor_id           BIGINT REFERENCES vendors(vendor_id),
    reorder_point       INTEGER,
    last_restock_date   DATE,
    PRIMARY KEY (dc_id, item_id)
);

CREATE TABLE dc_item_slotting (
    slotting_id          BIGSERIAL PRIMARY KEY,
    dc_id                BIGINT NOT NULL REFERENCES distribution_centers(dc_id),
    item_id              BIGINT NOT NULL REFERENCES items(item_id),
    pick_location        VARCHAR(50),
    replenishment_location VARCHAR(50),
    slotting_rank        INTEGER,
    velocity_class       CHAR(1),
    UNIQUE (dc_id, item_id)
);

CREATE TABLE carriers (
    carrier_id           BIGSERIAL PRIMARY KEY,
    carrier_name         VARCHAR(200) NOT NULL,
    carrier_type         VARCHAR(50) NOT NULL,
    service_level        VARCHAR(50),
    base_rate_per_lb     NUMERIC(12,4),
    base_rate_per_mile   NUMERIC(12,4),
    fuel_surcharge_pct   NUMERIC(6,2)
);

CREATE TABLE orders (
    order_id        BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customers(customer_id),
    order_date      DATE NOT NULL,
    dc_id           BIGINT REFERENCES distribution_centers(dc_id),
    ship_via        VARCHAR(50),
    order_status    VARCHAR(50)
);

CREATE TABLE order_lines (
    order_line_id     BIGSERIAL PRIMARY KEY,
    order_id          BIGINT NOT NULL REFERENCES orders(order_id),
    item_id           BIGINT NOT NULL REFERENCES items(item_id),
    quantity_ordered  INTEGER NOT NULL,
    quantity_shipped  INTEGER NOT NULL DEFAULT 0,
    unit_price        NUMERIC(12,4) NOT NULL,
    extended_price    NUMERIC(12,2)
);

CREATE TABLE shipments (
    shipment_id       BIGSERIAL PRIMARY KEY,
    order_id          BIGINT NOT NULL REFERENCES orders(order_id),
    dc_id             BIGINT REFERENCES distribution_centers(dc_id),
    carrier_id        BIGINT REFERENCES carriers(carrier_id),
    ship_date         DATE NOT NULL,
    promised_date     DATE,
    delivery_date     DATE,
    shipment_status   VARCHAR(50),
    shipment_type     VARCHAR(50),
    total_weight_lb   NUMERIC(12,4),
    total_cube_ft     NUMERIC(12,4),
    estimated_cost    NUMERIC(12,2),
    actual_cost       NUMERIC(12,2)
);

CREATE TABLE backorders (
    backorder_id       BIGSERIAL PRIMARY KEY,
    order_id           BIGINT NOT NULL REFERENCES orders(order_id),
    order_line_id      BIGINT NOT NULL REFERENCES order_lines(order_line_id),
    item_id            BIGINT NOT NULL REFERENCES items(item_id),
    qty_backordered    INTEGER NOT NULL,
    date_created       DATE NOT NULL,
    date_filled        DATE,
    days_backordered   INTEGER,
    reason_code        VARCHAR(100),
    is_filled_flag     BOOLEAN
);

CREATE TABLE customer_item_preferences (
    preference_id      BIGSERIAL PRIMARY KEY,
    customer_id        BIGINT NOT NULL REFERENCES customers(customer_id),
    item_id            BIGINT NOT NULL REFERENCES items(item_id),
    preference_score   NUMERIC(5,2),
    last_purchased_date DATE,
    UNIQUE (customer_id, item_id)
);

CREATE TABLE shipment_lines (
    shipment_line_id    BIGSERIAL PRIMARY KEY,
    shipment_id         BIGINT NOT NULL REFERENCES shipments(shipment_id),
    order_line_id       BIGINT NOT NULL REFERENCES order_lines(order_line_id),
    item_id             BIGINT NOT NULL REFERENCES items(item_id),
    quantity_shipped    INTEGER NOT NULL,
    carton_number       VARCHAR(100),
    line_weight_lb      NUMERIC(12,4),
    line_cube_ft        NUMERIC(12,4),
    freight_upcharge    NUMERIC(12,4),
    from_location_type  VARCHAR(50),
    from_location_id    BIGINT,
    to_location_type    VARCHAR(50),
    to_location_id      BIGINT,
    is_courier          BOOLEAN
);

CREATE TABLE tracking_events (
    tracking_event_id BIGSERIAL PRIMARY KEY,
    shipment_id       BIGINT NOT NULL REFERENCES shipments(shipment_id),
    event_timestamp   TIMESTAMP NOT NULL,
    event_type        VARCHAR(50),
    location_city     VARCHAR(100),
    location_state    CHAR(2),
    location_zip      VARCHAR(10),
    location_type     VARCHAR(50)
);

CREATE TABLE driver_costs (
    driver_cost_id BIGSERIAL PRIMARY KEY,
    carrier_id     BIGINT NOT NULL REFERENCES carriers(carrier_id),
    company_driver BOOLEAN,
    cost_type      VARCHAR(50),
    rate_amount    NUMERIC(12,4),
    effective_date DATE
);

CREATE TABLE fuel_costs (
    fuel_cost_id      BIGSERIAL PRIMARY KEY,
    region            VARCHAR(100),
    state             CHAR(2),
    fuel_type         VARCHAR(50),
    cost_per_gallon   NUMERIC(12,4),
    effective_date    DATE
);

CREATE TABLE invoices (
    invoice_id       BIGSERIAL PRIMARY KEY,
    invoice_number   VARCHAR(50) NOT NULL UNIQUE,
    order_id         BIGINT NOT NULL REFERENCES orders(order_id),
    customer_id      BIGINT NOT NULL REFERENCES customers(customer_id),
    invoice_date     DATE NOT NULL,
    due_date         DATE,
    total_amount     NUMERIC(12,2) NOT NULL,
    freight_billed   NUMERIC(12,2),
    tax_amount       NUMERIC(12,2),
    discount_amount  NUMERIC(12,2),
    payment_terms    VARCHAR(50)
);

CREATE TABLE ar_ledger (
    ar_entry_id      BIGSERIAL PRIMARY KEY,
    invoice_id       BIGINT NOT NULL REFERENCES invoices(invoice_id),
    customer_id      BIGINT NOT NULL REFERENCES customers(customer_id),
    posting_date     DATE NOT NULL,
    amount_due       NUMERIC(12,2) NOT NULL,
    amount_paid      NUMERIC(12,2) NOT NULL DEFAULT 0,
    balance          NUMERIC(12,2),
    aging_bucket     VARCHAR(50),
    credit_hold_flag BOOLEAN DEFAULT FALSE,
    writeoff_reason  VARCHAR(100)
);

CREATE TABLE sales_reps (
    rep_id      BIGSERIAL    PRIMARY KEY,
    rep_name    VARCHAR(200) NOT NULL,
    email       VARCHAR(200),
    region      VARCHAR(50),
    territory   VARCHAR(100),
    hire_date   DATE
);

CREATE TABLE customer_contacts (
    contact_id        BIGSERIAL    PRIMARY KEY,
    customer_id       BIGINT       NOT NULL REFERENCES customers(customer_id),
    rep_id            BIGINT       REFERENCES sales_reps(rep_id),
    contact_name      VARCHAR(200),
    role              VARCHAR(100),
    email             VARCHAR(200),
    last_contact_date DATE
);

CREATE TABLE purchase_orders (
    po_id                 BIGSERIAL   PRIMARY KEY,
    vendor_id             BIGINT      NOT NULL REFERENCES vendors(vendor_id),
    item_id               BIGINT      NOT NULL REFERENCES items(item_id),
    dc_id                 BIGINT      NOT NULL REFERENCES distribution_centers(dc_id),
    po_number             VARCHAR(50) NOT NULL UNIQUE,
    po_date               DATE        NOT NULL,
    expected_receipt_date DATE,
    actual_receipt_date   DATE,
    qty_ordered           INTEGER     NOT NULL,
    qty_received          INTEGER,
    po_unit_cost          NUMERIC(12,4),
    po_status             VARCHAR(30)           -- Received / Partial / Open / Cancelled
);

CREATE TABLE price_history (
    price_id       BIGSERIAL    PRIMARY KEY,
    item_id        BIGINT       NOT NULL REFERENCES items(item_id),
    customer_id    BIGINT       REFERENCES customers(customer_id),
    effective_date DATE         NOT NULL,
    end_date       DATE,                        -- NULL for currently active record
    list_price     NUMERIC(12,4) NOT NULL,
    discount_pct   NUMERIC(6,3) DEFAULT 0,
    net_price      NUMERIC(12,4),
    promo_flag     BOOLEAN      DEFAULT FALSE,
    channel        VARCHAR(50)
);

CREATE TABLE promotions (
    promo_id         BIGSERIAL    PRIMARY KEY,
    promo_name       VARCHAR(200),
    promo_code       VARCHAR(50)  UNIQUE,
    start_date       DATE         NOT NULL,
    end_date         DATE         NOT NULL,
    discount_type    VARCHAR(30),              -- PCTOFF / FIXEDOFF / BOGO
    discount_value   NUMERIC(8,4),
    applies_to       VARCHAR(30),              -- CATEGORY / ITEM
    category         VARCHAR(100),
    customer_segment VARCHAR(50)               -- NULL = applies to all segments
);

CREATE TABLE promo_orders (
    promo_order_id BIGSERIAL PRIMARY KEY,
    promo_id       BIGINT    NOT NULL REFERENCES promotions(promo_id),
    order_id       BIGINT    NOT NULL REFERENCES orders(order_id),
    UNIQUE (promo_id, order_id)
);

CREATE TABLE returns (
    return_id      BIGSERIAL    PRIMARY KEY,
    order_id       BIGINT       NOT NULL REFERENCES orders(order_id),
    order_line_id  BIGINT       NOT NULL REFERENCES order_lines(order_line_id),
    shipment_id    BIGINT       REFERENCES shipments(shipment_id),
    item_id        BIGINT       NOT NULL REFERENCES items(item_id),
    customer_id    BIGINT       NOT NULL REFERENCES customers(customer_id),
    return_date    DATE         NOT NULL,
    reason_code    VARCHAR(50),               -- Damaged / Wrong Item / Not as Described /
                                              -- Overshipment / Customer Preference / Defective
    condition_code VARCHAR(50),               -- Resalable / Damaged / Defective / Destroyed
    qty_returned   INTEGER      NOT NULL,
    restock_flag   BOOLEAN      DEFAULT FALSE,
    credit_amount  NUMERIC(12,2)
);

CREATE TABLE labor_costs (
    labor_id        BIGSERIAL    PRIMARY KEY,
    dc_id           BIGINT       NOT NULL REFERENCES distribution_centers(dc_id),
    week_start_date DATE         NOT NULL,
    department      VARCHAR(50),
    role            VARCHAR(50),
    labor_type      VARCHAR(30),              -- Regular / Overtime
    hours_worked    NUMERIC(8,2),
    cost_per_hour   NUMERIC(8,2),
    total_cost      NUMERIC(12,2),
    headcount       INTEGER
);

-- =======================================
-- INDEXES (TABLES)
-- =======================================

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_dc_id ON orders(dc_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);

CREATE INDEX idx_order_lines_order_id ON order_lines(order_id);
CREATE INDEX idx_order_lines_item_id ON order_lines(item_id);
CREATE INDEX idx_order_lines_short_ship ON order_lines(quantity_shipped, quantity_ordered);

CREATE INDEX idx_items_item_sku ON items(item_sku);

CREATE INDEX idx_inventory_snapshot_dc_item_date
    ON inventory_monthly_snapshot(dc_id, item_id, snapshot_month DESC);

CREATE INDEX idx_inventory_dc_item ON inventory(dc_id, item_id);

CREATE INDEX idx_shipments_carrier_id ON shipments(carrier_id);
CREATE INDEX idx_shipments_order_id ON shipments(order_id);
CREATE INDEX idx_shipments_dc_id ON shipments(dc_id);
CREATE INDEX idx_shipments_dates ON shipments(ship_date, delivery_date);

CREATE INDEX idx_shipment_lines_shipment_id ON shipment_lines(shipment_id);
CREATE INDEX idx_shipment_lines_order_line_id ON shipment_lines(order_line_id);

CREATE INDEX idx_tracking_events_shipment_id ON tracking_events(shipment_id);
CREATE INDEX idx_tracking_events_timestamp ON tracking_events(event_timestamp);

CREATE INDEX idx_invoices_order_id ON invoices(order_id);
CREATE INDEX idx_invoices_customer_id ON invoices(customer_id);
CREATE INDEX idx_ar_ledger_customer_id ON ar_ledger(customer_id);
CREATE INDEX idx_ar_ledger_invoice_id ON ar_ledger(invoice_id);

CREATE INDEX idx_vendor_item_details_vendor_item
    ON vendor_item_details(vendor_id, item_id);

CREATE INDEX idx_item_costs_item_effective_date
    ON item_costs(item_id, effective_date DESC);

CREATE INDEX idx_item_forecast_item_month
    ON item_forecast(item_id, forecast_month);

CREATE INDEX idx_backorders_order_id ON backorders(order_id);
CREATE INDEX idx_backorders_item_id ON backorders(item_id);

CREATE INDEX idx_customer_contacts_customer_id
    ON customer_contacts (customer_id);
CREATE INDEX idx_customer_contacts_rep_id
    ON customer_contacts (rep_id);

CREATE INDEX idx_purchase_orders_vendor_id
    ON purchase_orders (vendor_id);
CREATE INDEX idx_purchase_orders_item_id
    ON purchase_orders (item_id);
CREATE INDEX idx_purchase_orders_dc_id
    ON purchase_orders (dc_id);
CREATE INDEX idx_purchase_orders_status
    ON purchase_orders (po_status);
CREATE INDEX idx_purchase_orders_dates
    ON purchase_orders (po_date, expected_receipt_date);

CREATE INDEX idx_price_history_item_id
    ON price_history (item_id);
CREATE INDEX idx_price_history_customer_id
    ON price_history (customer_id)
    WHERE customer_id IS NOT NULL;
CREATE INDEX idx_price_history_effective_date
    ON price_history (item_id, effective_date DESC);

CREATE INDEX idx_promotions_dates
    ON promotions (start_date, end_date);
CREATE INDEX idx_promotions_code
    ON promotions (promo_code);

CREATE INDEX idx_promo_orders_promo_id
    ON promo_orders (promo_id);
CREATE INDEX idx_promo_orders_order_id
    ON promo_orders (order_id);

CREATE INDEX idx_returns_order_id
    ON returns (order_id);
CREATE INDEX idx_returns_order_line_id
    ON returns (order_line_id);
CREATE INDEX idx_returns_item_id
    ON returns (item_id);
CREATE INDEX idx_returns_customer_id
    ON returns (customer_id);
CREATE INDEX idx_returns_return_date
    ON returns (return_date);

CREATE INDEX idx_labor_costs_dc_id
    ON labor_costs (dc_id);
CREATE INDEX idx_labor_costs_dc_week
    ON labor_costs (dc_id, week_start_date);
CREATE INDEX idx_labor_costs_department
    ON labor_costs (department);
