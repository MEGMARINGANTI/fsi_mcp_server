"""
FSI Margin Intelligence — MCP Server
Connects Claude.ai to FSI's Snowflake data (FSI_MARGIN_BASE_MAT + views)
Hosted on Render, called by Claude.ai via remote MCP over HTTPS
"""

import os
import json
import snowflake.connector
from mcp.server.fastmcp import FastMCP

# ── Server init ───────────────────────────────────────────
mcp = FastMCP("FSI Margin Intelligence")

# ── Snowflake connection ──────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "FSI_MARGIN_NIGHTLYREPORTING"),
        database  = os.environ.get("SNOWFLAKE_DATABASE", "NETSUITE"),
        schema    = os.environ.get("SNOWFLAKE_SCHEMA",   "FINANCIALS"),
        role      = os.environ.get("SNOWFLAKE_ROLE",     "ACCOUNTADMIN"),
    )

def run_query(sql: str, params: tuple = None) -> list[dict]:
    """Run a query and return results as a list of dicts."""
    conn = get_conn()
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════
# TOOL 1 — MARGIN SUMMARY
# "How's margin this month?" / "What's our blended vs core?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_margin_summary() -> str:
    """
    Returns the current and comparison period margin summary —
    blended margin, core margin (ex-TFA), TFA drag, revenue per day,
    GP per day, and day counts for both periods.
    Use this for any question about overall margin performance.
    """
    rows = run_query("""
        SELECT
            PERIOD_LABEL,
            PERIOD_MONTH,
            PERIOD_DAYS_CALC,
            BLENDED_REVENUE,
            BLENDED_GP,
            BLENDED_MARGIN_PCT,
            CORE_REVENUE,
            CORE_GP,
            CORE_MARGIN_PCT,
            TFA_GP,
            TFA_DRAG_PP,
            REVENUE_PER_DAY,
            GP_PER_DAY,
            CORE_GP_PER_DAY
        FROM NETSUITE.FINANCIALS.V_MARGIN_SUMMARY
        ORDER BY PERIOD_MONTH
    """)
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 2 — MARGIN HISTORY (uses FSI_MARGIN_BASE_MAT directly)
# "How has margin trended over the last 6 months?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_margin_history(months: int = 6) -> str:
    """
    Returns monthly blended and core margin trend for the last N months
    (default 6). Use for trend questions, year-over-year, seasonality.
    months: number of months of history to return (1-24)
    """
    months = max(1, min(24, months))
    rows = run_query("""
        SELECT
            PERIOD_MONTH,
            COUNT(*)                                                    AS ROW_COUNT,
            SUM(SALES_AMT)                                              AS REVENUE,
            SUM(COGS_AMT)                                               AS COGS,
            SUM(SALES_AMT - COGS_AMT)                                   AS GP,
            ROUND(SUM(SALES_AMT - COGS_AMT)
                / NULLIF(SUM(SALES_AMT), 0) * 100, 2)                  AS BLENDED_MARGIN_PCT,
            ROUND(
                SUM(CASE WHEN NOT IS_TFA AND NOT IS_SERVICE AND NOT IS_PERIOD_CROSSOVER
                    THEN SALES_AMT - COGS_AMT ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NOT IS_TFA AND NOT IS_SERVICE AND NOT IS_PERIOD_CROSSOVER
                    THEN SALES_AMT ELSE 0 END), 0) * 100, 2)           AS CORE_MARGIN_PCT,
            ROUND(
                SUM(CASE WHEN IS_TFA THEN SALES_AMT - COGS_AMT ELSE 0 END)
                / NULLIF(SUM(SALES_AMT), 0) * 100, 2)                  AS TFA_DRAG_PP
        FROM NETSUITE.FINANCIALS.FSI_MARGIN_BASE_MAT
        WHERE PERIOD_MONTH >= TO_VARCHAR(
            DATEADD('month', -%s, DATE_TRUNC('month', CURRENT_DATE())),
            'YYYY-MM'
        )
        GROUP BY PERIOD_MONTH
        ORDER BY PERIOD_MONTH DESC
    """, (months,))
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 3 — CHANNEL PERFORMANCE
# "How's Construction doing?" / "Which channel has best margin?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_channel_performance() -> str:
    """
    Returns core margin by channel (Industrial, Construction, Dealer, Other)
    for both the current and comparison period, with revenue share and daily rates.
    Use for any channel-level question.
    """
    rows = run_query("""
        SELECT
            PERIOD_LABEL,
            CUSTOMER_TYPE                                               AS CHANNEL,
            ROUND(SUM(REVENUE), 0)                                      AS REVENUE,
            ROUND(SUM(GROSS_PROFIT), 0)                                 AS GP,
            ROUND(SUM(GROSS_PROFIT) / NULLIF(SUM(REVENUE), 0) * 100, 2) AS MARGIN_PCT,
            ROUND(SUM(REVENUE_SHARE_PCT), 2)                            AS REVENUE_SHARE_PCT,
            ROUND(SUM(REVENUE_PER_DAY), 0)                              AS REVENUE_PER_DAY,
            ROUND(SUM(GP_PER_DAY), 0)                                   AS GP_PER_DAY
        FROM NETSUITE.FINANCIALS.V_CHANNEL_MARGIN
        GROUP BY PERIOD_LABEL, CUSTOMER_TYPE
        ORDER BY PERIOD_LABEL, SUM(REVENUE) DESC
    """)
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 4 — SUBGROUP PERFORMANCE
# "How are Single-Family Framers doing?" / "Show me subgroup breakdown"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_subgroup_performance(channel: str = None) -> str:
    """
    Returns margin by customer subgroup for both periods.
    Optionally filter by channel name (e.g. 'CONSTRUCTION', 'INDUSTRIAL', 'DEALER').
    Use for subgroup-level questions or drilling into a specific channel.
    channel: optional filter — CONSTRUCTION, INDUSTRIAL, DEALER, or OTHER
    """
    if channel:
        rows = run_query("""
            SELECT
                PERIOD_LABEL,
                CUSTOMER_TYPE       AS CHANNEL,
                CUSTOMER_SUBTYPE    AS SUBGROUP,
                ROUND(SUM(REVENUE), 0)                                       AS REVENUE,
                ROUND(SUM(GROSS_PROFIT), 0)                                  AS GP,
                ROUND(SUM(GROSS_PROFIT)/NULLIF(SUM(REVENUE),0)*100, 2)      AS MARGIN_PCT,
                ROUND(SUM(GP_PER_DAY), 0)                                    AS GP_PER_DAY
            FROM NETSUITE.FINANCIALS.V_CHANNEL_MARGIN
            WHERE UPPER(CUSTOMER_TYPE) = UPPER(%s)
            GROUP BY 1, 2, 3
            ORDER BY PERIOD_LABEL, SUM(REVENUE) DESC
        """, (channel,))
    else:
        rows = run_query("""
            SELECT
                PERIOD_LABEL,
                CUSTOMER_TYPE       AS CHANNEL,
                CUSTOMER_SUBTYPE    AS SUBGROUP,
                ROUND(SUM(REVENUE), 0)                                       AS REVENUE,
                ROUND(SUM(GROSS_PROFIT), 0)                                  AS GP,
                ROUND(SUM(GROSS_PROFIT)/NULLIF(SUM(REVENUE),0)*100, 2)      AS MARGIN_PCT,
                ROUND(SUM(GP_PER_DAY), 0)                                    AS GP_PER_DAY
            FROM NETSUITE.FINANCIALS.V_CHANNEL_MARGIN
            GROUP BY 1, 2, 3
            ORDER BY PERIOD_LABEL, CUSTOMER_TYPE, SUM(REVENUE) DESC
        """)
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 5 — REGIONAL PERFORMANCE
# "How's Texas doing?" / "Which region is gaining share?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_regional_performance() -> str:
    """
    Returns core margin by region for both periods with revenue share.
    Use for regional questions, territory performance, or share shifts.
    """
    rows = run_query("""
        SELECT
            PERIOD_LABEL,
            PRICE_BRANCH_REGION                                         AS REGION,
            ROUND(SUM(REVENUE), 0)                                      AS REVENUE,
            ROUND(SUM(GROSS_PROFIT), 0)                                 AS GP,
            ROUND(SUM(GROSS_PROFIT)/NULLIF(SUM(REVENUE),0)*100, 2)     AS MARGIN_PCT,
            ROUND(SUM(REVENUE_SHARE_PCT), 2)                            AS REVENUE_SHARE_PCT,
            ROUND(SUM(REVENUE_PER_DAY), 0)                              AS REVENUE_PER_DAY
        FROM NETSUITE.FINANCIALS.V_REGIONAL_MARGIN
        WHERE PRICE_BRANCH_REGION != 'Corporate'
        GROUP BY 1, 2
        ORDER BY PERIOD_LABEL, SUM(REVENUE) DESC
    """)
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 6 — TFA ANALYSIS
# "Who got tools last month?" / "What's our TFA drag?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_tfa_analysis() -> str:
    """
    Returns TFA (Tool & Fastener Agreement) breakdown:
    GL category daily costs, placement segments, and top large placements
    with new vs repeat status. Use for any TFA-related question.
    """
    rows = run_query("""
        SELECT
            RECORD_TYPE,
            PERIOD_LABEL,
            PERIOD_MONTH,
            GL_CODE,
            PLACEMENT_SEGMENT,
            PARENTNAME          AS CUSTOMER,
            CUSTOMER_TYPE,
            PRICE_BRANCH_REGION AS REGION,
            CUSTOMER_COUNT,
            TFA_REVENUE,
            TFA_COGS,
            TFA_GP,
            TFA_GP_PER_DAY,
            IS_NEW_CUSTOMER
        FROM NETSUITE.FINANCIALS.V_TFA_DETAIL
        ORDER BY RECORD_TYPE, PERIOD_LABEL, TFA_GP ASC
    """)
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 7 — PRICE REALIZATION
# "Where are we leaving money on the table?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_price_realization() -> str:
    """
    Returns price realization analysis — GP dollars left on table vs
    suggested pricing, broken down by period summary, top products,
    and top customers. Use for pricing discipline questions.
    """
    rows = run_query("""
        SELECT
            RECORD_TYPE,
            PERIOD_LABEL,
            PERIOD_MONTH,
            ITEM_SN2,
            MASTER2,
            PARENTNAME          AS CUSTOMER,
            CUSTOMER_TYPE,
            CUSTOMER_TIER,
            PRICE_BRANCH_REGION AS REGION,
            ACTUAL_REVENUE,
            SUGGESTED_REVENUE,
            ACTUAL_GP,
            GP_OPPORTUNITY,
            REALIZATION_PCT,
            ACTUAL_MARGIN_PCT,
            SUGGESTED_MARGIN_PCT,
            PCT_REVENUE_LEFT_ON_TABLE,
            RANK_BY_OPPORTUNITY
        FROM NETSUITE.FINANCIALS.V_PRICE_REALIZATION
        ORDER BY RECORD_TYPE, PERIOD_LABEL, RANK_BY_OPPORTUNITY
    """)
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 8 — MARGIN BRIDGE
# "What drove the margin change?" / "Break down the waterfall"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_margin_bridge() -> str:
    """
    Returns the waterfall bridge decomposing the margin change between
    periods into: channel mix effect, within-channel rate effect,
    TFA impact change, and interaction/other. Use when asked what
    drove a margin increase or decline.
    """
    rows = run_query("""
        SELECT
            STEP,
            BRIDGE_COMPONENT,
            BRIDGE_VALUE_PP,
            BRIDGE_CATEGORY
        FROM NETSUITE.FINANCIALS.V_MARGIN_BRIDGE
        ORDER BY STEP
    """)
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 9 — ACCOUNT LOOKUP
# "Show me MAYO MFG's history" / "What's HM Richards buying?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_account_history(account_name: str, months: int = 6) -> str:
    """
    Returns purchase and margin history for a specific customer account.
    Searches by parent account name (partial match supported).
    account_name: customer name or partial name to search (e.g. 'MAYO', 'HM RICHARDS')
    months: how many months of history to return (default 6, max 24)
    """
    months = max(1, min(24, months))
    rows = run_query("""
        SELECT
            PERIOD_MONTH,
            PARENTNAME,
            CUSTOMER_TYPE,
            CUSTOMER_SUBTYPE,
            CUSTOMER_TIER,
            PRICE_BRANCH_REGION                                         AS REGION,
            SUM(SALES_AMT)                                              AS REVENUE,
            SUM(COGS_AMT)                                               AS COGS,
            SUM(SALES_AMT - COGS_AMT)                                   AS GP,
            ROUND(SUM(SALES_AMT - COGS_AMT)
                / NULLIF(SUM(SALES_AMT), 0) * 100, 2)                  AS MARGIN_PCT,
            SUM(CASE WHEN IS_TFA THEN SALES_AMT - COGS_AMT ELSE 0 END) AS TFA_GP,
            COUNT(DISTINCT ITEM_SN2)                                    AS DISTINCT_PRODUCTS,
            SUM(SALES_QTY)                                              AS TOTAL_QTY
        FROM NETSUITE.FINANCIALS.FSI_MARGIN_BASE_MAT
        WHERE UPPER(PARENTNAME) LIKE UPPER(%s)
            AND PERIOD_MONTH >= TO_VARCHAR(
                DATEADD('month', -%s, DATE_TRUNC('month', CURRENT_DATE())),
                'YYYY-MM'
            )
            AND NOT IS_TFA
            AND NOT IS_SERVICE
        GROUP BY 1, 2, 3, 4, 5, 6
        ORDER BY PERIOD_MONTH DESC, SUM(SALES_AMT) DESC
    """, (f"%{account_name}%", months))
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 10 — TFA CONVERSION TRACKER
# "Which tool placements haven't converted to revenue yet?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_tfa_conversion_status() -> str:
    """
    Returns all large TFA placements from the current period with their
    current revenue status — Growing, Flat/Declining, No Revenue Yet,
    or New Account. Critical for tracking ROI on tool investments.
    Use when asked about tool placement follow-up or TFA ROI.
    """
    rows = run_query("""
        WITH CFG AS (
            SELECT CURRENT_PERIOD, COMPARISON_PERIOD
            FROM NETSUITE.FINANCIALS.REPORT_CONTROL
        ),
        TFA AS (
            SELECT M.PARENTNAME, M.PRICE_BRANCH_REGION AS REGION,
                   SUM(M.SALES_AMT - M.COGS_AMT)       AS TFA_NET_GP
            FROM NETSUITE.FINANCIALS.FSI_MARGIN_BASE_MAT M CROSS JOIN CFG
            WHERE M.PERIOD_MONTH = CFG.CURRENT_PERIOD
                AND M.IS_TFA = TRUE AND M.GL_CODE = 'FASTENER TOOLS'
            GROUP BY 1, 2
            HAVING SUM(M.SALES_AMT - M.COGS_AMT) < -500
        ),
        CUR AS (
            SELECT M.PARENTNAME,
                   SUM(M.SALES_AMT)                     AS CORE_REVENUE,
                   ROUND(SUM(M.SALES_AMT - M.COGS_AMT)
                       / NULLIF(SUM(M.SALES_AMT),0)*100,2) AS CORE_MARGIN
            FROM NETSUITE.FINANCIALS.FSI_MARGIN_BASE_MAT M CROSS JOIN CFG
            WHERE M.PERIOD_MONTH = CFG.CURRENT_PERIOD
                AND NOT M.IS_TFA AND NOT M.IS_SERVICE AND NOT M.IS_PERIOD_CROSSOVER
            GROUP BY 1
        ),
        PRI AS (
            SELECT M.PARENTNAME, SUM(M.SALES_AMT) AS PRIOR_REVENUE
            FROM NETSUITE.FINANCIALS.FSI_MARGIN_BASE_MAT M CROSS JOIN CFG
            WHERE M.PERIOD_MONTH = CFG.COMPARISON_PERIOD
                AND NOT M.IS_TFA AND NOT M.IS_SERVICE AND NOT M.IS_PERIOD_CROSSOVER
            GROUP BY 1
        )
        SELECT
            T.PARENTNAME                                AS CUSTOMER,
            T.REGION,
            ROUND(ABS(T.TFA_NET_GP), 0)                AS PLACEMENT_COST,
            COALESCE(C.CORE_REVENUE, 0)                AS CURRENT_REVENUE,
            COALESCE(C.CORE_MARGIN, 0)                 AS CURRENT_MARGIN_PCT,
            COALESCE(P.PRIOR_REVENUE, 0)               AS PRIOR_REVENUE,
            CASE
                WHEN P.PARENTNAME IS NULL                   THEN 'New Account'
                WHEN COALESCE(C.CORE_REVENUE,0) = 0         THEN 'No Revenue Yet'
                WHEN COALESCE(C.CORE_REVENUE,0) >
                     COALESCE(P.PRIOR_REVENUE,0)            THEN 'Growing'
                ELSE 'Flat / Declining'
            END                                         AS REVENUE_STATUS
        FROM TFA T
        LEFT JOIN CUR C ON T.PARENTNAME = C.PARENTNAME
        LEFT JOIN PRI P ON T.PARENTNAME = P.PARENTNAME
        ORDER BY T.TFA_NET_GP ASC
    """)
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 11 — TOP CUSTOMERS BY REVENUE OR MARGIN
# "Who are our top 20 customers?" / "Biggest accounts in Texas?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_top_customers(
    period_month: str = None,
    region: str = None,
    channel: str = None,
    limit: int = 20
) -> str:
    """
    Returns top customers ranked by revenue for a given period.
    All filters are optional.
    period_month: e.g. '2026-02' — defaults to current period from REPORT_CONTROL
    region: e.g. 'Texas', 'South', 'East', 'Central', 'Coastal'
    channel: e.g. 'INDUSTRIAL', 'CONSTRUCTION', 'DEALER'
    limit: number of customers to return (default 20, max 100)
    """
    limit = max(1, min(100, limit))

    where_clauses = [
        "NOT IS_TFA", "NOT IS_SERVICE", "NOT IS_PERIOD_CROSSOVER"
    ]
    params = []

    if period_month:
        where_clauses.append("PERIOD_MONTH = %s")
        params.append(period_month)
    else:
        where_clauses.append("""
            PERIOD_MONTH = (
                SELECT CURRENT_PERIOD FROM NETSUITE.FINANCIALS.REPORT_CONTROL
            )
        """)

    if region:
        where_clauses.append("UPPER(PRICE_BRANCH_REGION) = UPPER(%s)")
        params.append(region)

    if channel:
        where_clauses.append("UPPER(CUSTOMER_TYPE) = UPPER(%s)")
        params.append(channel)

    params.append(limit)
    where_sql = " AND ".join(where_clauses)

    rows = run_query(f"""
        SELECT
            PARENTNAME,
            CUSTOMER_TYPE,
            CUSTOMER_SUBTYPE,
            CUSTOMER_TIER,
            PRICE_BRANCH_REGION                                         AS REGION,
            SUM(SALES_AMT)                                              AS REVENUE,
            SUM(COGS_AMT)                                               AS COGS,
            SUM(SALES_AMT - COGS_AMT)                                   AS GP,
            ROUND(SUM(SALES_AMT - COGS_AMT)
                / NULLIF(SUM(SALES_AMT), 0) * 100, 2)                  AS MARGIN_PCT,
            COUNT(DISTINCT ITEM_SN2)                                    AS DISTINCT_PRODUCTS
        FROM NETSUITE.FINANCIALS.FSI_MARGIN_BASE_MAT
        WHERE {where_sql}
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY SUM(SALES_AMT) DESC
        LIMIT %s
    """, tuple(params))
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 12 — PRODUCT PERFORMANCE
# "What are our best margin products?" / "How is JH14148 selling?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_product_performance(
    product_search: str = None,
    period_month: str = None,
    limit: int = 20
) -> str:
    """
    Returns product performance by item group (ITEM_SN2 / MASTER2).
    Optionally search by product name or code.
    product_search: partial product name or item code (e.g. 'JH14148', 'PT30120')
    period_month: e.g. '2026-02' — defaults to current period
    limit: number of products (default 20, max 50)
    """
    limit = max(1, min(50, limit))
    where_clauses = [
        "NOT IS_TFA", "NOT IS_SERVICE", "NOT IS_PERIOD_CROSSOVER",
        "SALES_AMT > 0"
    ]
    params = []

    if period_month:
        where_clauses.append("PERIOD_MONTH = %s")
        params.append(period_month)
    else:
        where_clauses.append("""
            PERIOD_MONTH = (
                SELECT CURRENT_PERIOD FROM NETSUITE.FINANCIALS.REPORT_CONTROL
            )
        """)

    if product_search:
        where_clauses.append(
            "(UPPER(ITEM_SN2) LIKE UPPER(%s) OR UPPER(ITEM_NAME) LIKE UPPER(%s) OR UPPER(MASTER2) LIKE UPPER(%s))"
        )
        pct = f"%{product_search}%"
        params.extend([pct, pct, pct])

    params.append(limit)
    where_sql = " AND ".join(where_clauses)

    rows = run_query(f"""
        SELECT
            ITEM_SN2,
            ITEM_NAME,
            MASTER2,
            GL_CODE,
            SUM(SALES_AMT)                                              AS REVENUE,
            SUM(COGS_AMT)                                               AS COGS,
            SUM(SALES_AMT - COGS_AMT)                                   AS GP,
            ROUND(SUM(SALES_AMT - COGS_AMT)
                / NULLIF(SUM(SALES_AMT), 0) * 100, 2)                  AS MARGIN_PCT,
            SUM(SALES_QTY)                                              AS TOTAL_QTY,
            COUNT(DISTINCT PARENTNAME)                                  AS CUSTOMER_COUNT
        FROM NETSUITE.FINANCIALS.FSI_MARGIN_BASE_MAT
        WHERE {where_sql}
        GROUP BY 1, 2, 3, 4
        ORDER BY SUM(SALES_AMT) DESC
        LIMIT %s
    """, tuple(params))
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# TOOL 13 — CURRENT CONFIG
# "What period is the report currently set to?"
# ════════════════════════════════════════════════════════════
@mcp.tool()
def get_report_config() -> str:
    """
    Returns the current REPORT_CONTROL settings — which periods are
    being compared, recipient list, and whether the daily task is active.
    Use when asked about the current reporting period or configuration.
    """
    rows = run_query("""
        SELECT
            CURRENT_PERIOD,
            COMPARISON_PERIOD,
            REPORT_RECIPIENTS,
            IS_ACTIVE,
            SEND_ON_WEEKENDS,
            LAST_UPDATED_AT
        FROM NETSUITE.FINANCIALS.REPORT_CONTROL
    """)
    return json.dumps(rows, default=str)


# ════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    mcp.run(transport="streamable-http")
