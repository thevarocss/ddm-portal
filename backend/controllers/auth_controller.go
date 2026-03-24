package controllers

import (
    "context"
    "crypto/rand"
    "encoding/hex"
    "net/http"
    "os"
    "time"

    "ddm-portal-backend/services"

    "github.com/gin-gonic/gin"
    "github.com/jackc/pgx/v5/pgxpool"
)

type RegisterRequest struct {
    Username string `json:"username"`
    Email    string `json:"email"`
    Password string `json:"password"`
    Role     string `json:"role"`
}

type LoginRequest struct {
    Login    string `json:"login"`
    Username string `json:"username"`
    Email    string `json:"email"`
    Password string `json:"password"`
}

type RefreshRequest struct {
    RefreshToken string `json:"refresh_token"`
}

type PasswordResetRequest struct {
    Email string `json:"email"`
}

type PasswordResetConfirmRequest struct {
    Token       string `json:"token"`
    NewPassword string `json:"new_password"`
}

func RegisterHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        var req RegisterRequest
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
            return
        }

        hash, _ := services.HashPassword(req.Password)

        _, err := pool.Exec(context.Background(),
            `INSERT INTO users (username, email, password_hash, role)
             VALUES ($1, $2, $3, $4)`,
            req.Username, req.Email, hash, req.Role,
        )
        if err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "username or email already exists"})
            return
        }

        c.JSON(http.StatusOK, gin.H{"message": "user registered"})
    }
}

func LoginHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        var req LoginRequest
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
            return
        }

        loginValue := req.Login
        if loginValue == "" {
            loginValue = req.Username
        }
        if loginValue == "" {
            loginValue = req.Email
        }

        if loginValue == "" {
            c.JSON(http.StatusBadRequest, gin.H{"error": "login, username, or email is required"})
            return
        }

        var id int64
        var username, email, hash, role string

        err := pool.QueryRow(context.Background(),
            `SELECT id, username, email, password_hash, role
             FROM users
             WHERE username=$1 OR email=$1`,
            loginValue,
        ).Scan(&id, &username, &email, &hash, &role)

        if err != nil || !services.CheckPasswordHash(req.Password, hash) {
            c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
            return
        }

        access, refresh, err := services.GenerateTokens(id, username, email, role)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "token generation failed"})
            return
        }

        c.JSON(http.StatusOK, gin.H{
            "access_token":  access,
            "refresh_token": refresh,
            "username":      username,
            "email":         email,
            "role":          role,
        })
    }
}

func MeHandler() gin.HandlerFunc {
    return func(c *gin.Context) {
        c.JSON(http.StatusOK, gin.H{
            "id":       c.GetInt64("user_id"),
            "username": c.GetString("username"),
            "email":    c.GetString("email"),
            "role":     c.GetString("role"),
        })
    }
}

func RefreshTokenHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        var req RefreshRequest
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
            return
        }

        claims, err := services.ParseRefreshToken(req.RefreshToken)
        if err != nil {
            c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid refresh token"})
            return
        }

        var username, email, role string
        err = pool.QueryRow(context.Background(),
            `SELECT username, email, role FROM users WHERE id=$1`,
            claims.ID,
        ).Scan(&username, &email, &role)

        if err != nil {
            c.JSON(http.StatusUnauthorized, gin.H{"error": "user not found"})
            return
        }

        access, refresh, err := services.GenerateTokens(claims.ID, username, email, role)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate new tokens"})
            return
        }

        c.JSON(http.StatusOK, gin.H{
            "access_token":  access,
            "refresh_token": refresh,
        })
    }
}

func generateResetToken() (string, error) {
    b := make([]byte, 32)
    _, err := rand.Read(b)
    if err != nil {
        return "", err
    }
    return hex.EncodeToString(b), nil
}

func RequestPasswordResetHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        var req PasswordResetRequest
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
            return
        }

        var userID int64
        err := pool.QueryRow(context.Background(),
            `SELECT id FROM users WHERE email=$1`,
            req.Email,
        ).Scan(&userID)

        if err != nil {
            c.JSON(http.StatusOK, gin.H{"message": "if email exists, reset link sent"})
            return
        }

        token, _ := generateResetToken()

        pool.Exec(context.Background(),
            `INSERT INTO password_resets (user_id, token, expires_at)
             VALUES ($1, $2, $3)`,
            userID, token, time.Now().Add(1*time.Hour),
        )

        resetURL := os.Getenv("FRONTEND_URL") + "/reset-password?token=" + token
        subject, body := services.BuildPasswordResetEmail(resetURL)
        services.SendMail(req.Email, subject, body)

        c.JSON(http.StatusOK, gin.H{"message": "if email exists, reset link sent"})
    }
}

func ConfirmPasswordResetHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        var req PasswordResetConfirmRequest
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
            return
        }

        var userID int64
        var expires time.Time
        var used bool

        err := pool.QueryRow(context.Background(),
            `SELECT user_id, expires_at, used
             FROM password_resets
             WHERE token=$1`,
            req.Token,
        ).Scan(&userID, &expires, &used)

        if err != nil || used || time.Now().After(expires) {
            c.JSON(http.StatusBadRequest, gin.H{"error": "invalid or expired token"})
            return
        }

        hash, _ := services.HashPassword(req.NewPassword)

        pool.Exec(context.Background(),
            `UPDATE users SET password_hash=$1 WHERE id=$2`,
            hash, userID,
        )

        pool.Exec(context.Background(),
            `UPDATE password_resets SET used=true WHERE token=$1`,
            req.Token,
        )

        c.JSON(http.StatusOK, gin.H{"message": "password updated"})
    }
}
