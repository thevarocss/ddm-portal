// controllers/subscription_controller.go
package controllers

import (
    "context"
    "net/http"
    "time"

    "github.com/gin-gonic/gin"
    "github.com/jackc/pgx/v5/pgxpool"
)

func SubscriptionTrendsHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        rows, err := pool.Query(context.Background(), `
            SELECT day, subscriptions
            FROM mv_daily_subscriptions
            ORDER BY day DESC
            LIMIT 30
        `)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }
        defer rows.Close()

        type SubscriptionData struct {
            Day   time.Time `json:"day"`
            Count int       `json:"count"`
        }
        var results []SubscriptionData

        for rows.Next() {
            var d SubscriptionData
            if err := rows.Scan(&d.Day, &d.Count); err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
                return
            }
            results = append(results, d)
        }

        c.JSON(http.StatusOK, results)
    }
}
