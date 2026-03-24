// middleware/roles_middleware.go
package middleware

import (
    "net/http"

    "github.com/gin-gonic/gin"
)

func RequireRole(roles ...string) gin.HandlerFunc {
    return func(c *gin.Context) {
        userRole := c.GetString("role")
        for _, r := range roles {
            if userRole == r {
                c.Next()
                return
            }
        }
        c.JSON(http.StatusForbidden, gin.H{"error": "forbidden: insufficient permissions"})
        c.Abort()
    }
}
