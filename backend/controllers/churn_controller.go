// controllers/churn_controller.go
package controllers

import (
    "context"
    "net/http"
    "time"

    "github.com/gin-gonic/gin"
    "github.com/jackc/pgx/v5/pgxpool"
)

type ChurnData struct {
    Day            time.Time `json:"day"`
    Subscriptions  int       `json:"subscriptions"`
    Unsubscriptions int      `json:"unsubscriptions"`
}

func ChurnHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        rows, err := pool.Query(context.Background(), `
            SELECT day, subscriptions, unsubscriptions
            FROM mv_churn_daily
            ORDER BY day DESC
            LIMIT 30
        `)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }
        defer rows.Close()

        var results []ChurnData
        for rows.Next() {
            var d ChurnData
            if err := rows.Scan(&d.Day, &d.Subscriptions, &d.Unsubscriptions); err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
                return
            }
            results = append(results, d)
        }

        c.JSON(http.StatusOK, results)
    }
}
