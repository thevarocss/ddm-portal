// middleware/auth_middleware.go
package middleware

import (
    "net/http"
    "strings"

    "ddm-portal-backend/services"
    "github.com/gin-gonic/gin"
)

func RequireAuth() gin.HandlerFunc {
    return func(c *gin.Context) {
        authHeader := c.GetHeader("Authorization")
        if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
            c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid Authorization header"})
            c.Abort()
            return
        }

        tokenString := strings.TrimPrefix(authHeader, "Bearer ")

        claims, err := services.ParseAccessToken(tokenString)
        if err != nil {
            c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
            c.Abort()
            return
        }

        c.Set("user_id", claims.ID)
        c.Set("username", claims.Username)
        c.Set("email", claims.Email)
        c.Set("role", claims.Role)

        c.Next()
    }
}
