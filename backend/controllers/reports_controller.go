// controllers/reports_controller.go
package controllers

import (
    "context"
    "net/http"
    "time"
    "fmt"

    "github.com/gin-gonic/gin"
    "github.com/jackc/pgx/v5/pgxpool"
)

// ExportReportsHandler generates a simple CSV export of subscription/unsubscription/revenue data
func ExportReportsHandler(pool *pgxpool.Pool) gin.HandlerFunc {
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
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }
        defer rows.Close()

        // Build CSV string
        csv := "day,subscriptions,unsubscriptions,total_revenue,active_subscribers\n"
        for rows.Next() {
            var day time.Time
            var subs, unsubs, active int
            var revenue float64
            if err := rows.Scan(&day, &subs, &unsubs, &revenue, &active); err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
                return
            }
            csv += day.Format("2006-01-02") + "," +
                fmt.Sprintf("%d,%d,%.2f,%d\n", subs, unsubs, revenue, active)
        }

        c.Header("Content-Type", "text/csv")
        c.Header("Content-Disposition", "attachment; filename=reports.csv")
        c.String(http.StatusOK, csv)
    }
}
