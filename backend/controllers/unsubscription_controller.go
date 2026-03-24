// controllers/unsubscription_controller.go
package controllers

import (
    "context"
    "net/http"
    "time"

    "github.com/gin-gonic/gin"
    "github.com/jackc/pgx/v5/pgxpool"
)

func UnsubscriptionTrendsHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        rows, err := pool.Query(context.Background(), `
            SELECT day, unsubscriptions
            FROM mv_daily_unsubscriptions
            ORDER BY day DESC
            LIMIT 30
        `)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }
        defer rows.Close()

        type UnsubscriptionData struct {
            Day   time.Time `json:"day"`
            Count int       `json:"count"`
        }
        var results []UnsubscriptionData

        for rows.Next() {
            var d UnsubscriptionData
            if err := rows.Scan(&d.Day, &d.Count); err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
                return
            }
            results = append(results, d)
        }

        c.JSON(http.StatusOK, results)
    }
}
