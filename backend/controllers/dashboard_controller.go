// controllers/dashboard_controller.go
package controllers

import (
    "context"
    "net/http"
    "time"

    "github.com/gin-gonic/gin"
    "github.com/jackc/pgx/v5/pgxpool"
)

// Daily revenue only
func DailyRevenueHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        rows, err := pool.Query(context.Background(),
            `SELECT day, total_revenue FROM mv_daily_revenue ORDER BY day DESC LIMIT 30`)
        if err != nil {
            c.Header("Cache-Control", "no-store")
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }
        defer rows.Close()

        var results []map[string]interface{}
        for rows.Next() {
            var day time.Time
            var revenue float64
            if err := rows.Scan(&day, &revenue); err != nil {
                c.Header("Cache-Control", "no-store")
                c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
                return
            }
            results = append(results, gin.H{
                "day":           day,
                "total_revenue": revenue,
            })
        }

        // Prevent caching
        c.Header("Cache-Control", "no-store")
        c.Header("Pragma", "no-cache")
        c.Header("Expires", "0")

        c.JSON(http.StatusOK, results)
    }
}

// Current active subscribers snapshot
func ActiveSubscribersHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        var count int
        err := pool.QueryRow(context.Background(),
            `SELECT COUNT(*) FROM mv_active_subscribers`).Scan(&count)
        if err != nil {
            c.Header("Cache-Control", "no-store")
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }

        // Prevent caching
        c.Header("Cache-Control", "no-store")
        c.Header("Pragma", "no-cache")
        c.Header("Expires", "0")

        c.JSON(http.StatusOK, gin.H{"active_subscribers": count})
    }
}

// Combined dashboard (subscriptions, unsubscriptions, revenue, historical active subscribers)
func DashboardHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        rows, err := pool.Query(context.Background(), `
            SELECT
                d.day,
                COALESCE(s.subscriptions, 0)     AS subscriptions,
                COALESCE(u.unsubscriptions, 0)   AS unsubscriptions,
                COALESCE(r.total_revenue, 0)     AS total_revenue,
                COALESCE(a.active_count, 0)      AS active_subscribers
            FROM (
                SELECT generate_series(
                    CURRENT_DATE - interval '30 days',
                    CURRENT_DATE,
                    interval '1 day'
                )::date AS day
            ) d
            LEFT JOIN mv_daily_subscriptions   s ON d.day = s.day
            LEFT JOIN mv_daily_unsubscriptions u ON d.day = u.day
            LEFT JOIN mv_daily_revenue         r ON d.day = r.day
            LEFT JOIN active_subscribers_history a ON d.day = a.day
            ORDER BY d.day DESC;
        `)
        if err != nil {
            c.Header("Cache-Control", "no-store")
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }
        defer rows.Close()

        type DashboardData struct {
            Day               time.Time `json:"day"`
            Subscriptions     int       `json:"subscriptions"`
            Unsubscriptions   int       `json:"unsubscriptions"`
            TotalRevenue      float64   `json:"total_revenue"`
            ActiveSubscribers int       `json:"active_subscribers"`
        }

        var results []DashboardData

        for rows.Next() {
            var d DashboardData
            if err := rows.Scan(&d.Day, &d.Subscriptions, &d.Unsubscriptions, &d.TotalRevenue, &d.ActiveSubscribers); err != nil {
                c.Header("Cache-Control", "no-store")
                c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
                return
            }
            results = append(results, d)
        }

        // Prevent caching
        c.Header("Cache-Control", "no-store")
        c.Header("Pragma", "no-cache")
        c.Header("Expires", "0")

        c.JSON(http.StatusOK, results)
    }
}
