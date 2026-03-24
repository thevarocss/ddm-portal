// services/jwt_service.go
package services

import (
    "os"
    "time"

    "github.com/golang-jwt/jwt/v5"
)

type AccessTokenClaims struct {
    ID       int64  `json:"id"`
    Username string `json:"username"`
    Email    string `json:"email"`
    Role     string `json:"role"`
    jwt.RegisteredClaims
}

type RefreshTokenClaims struct {
    ID int64 `json:"id"`
    jwt.RegisteredClaims
}

var accessSecret = []byte(os.Getenv("JWT_ACCESS_SECRET"))
var refreshSecret = []byte(os.Getenv("JWT_REFRESH_SECRET"))

func GenerateTokens(id int64, username, email, role string) (string, string, error) {
    now := time.Now()

    accessClaims := AccessTokenClaims{
        ID:       id,
        Username: username,
        Email:    email,
        Role:     role,
        RegisteredClaims: jwt.RegisteredClaims{
            ExpiresAt: jwt.NewNumericDate(now.Add(15 * time.Minute)),
            IssuedAt:  jwt.NewNumericDate(now),
        },
    }

    refreshClaims := RefreshTokenClaims{
        ID: id,
        RegisteredClaims: jwt.RegisteredClaims{
            ExpiresAt: jwt.NewNumericDate(now.Add(7 * 24 * time.Hour)),
            IssuedAt:  jwt.NewNumericDate(now),
        },
    }

    accessToken := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)
    refreshToken := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims)

    accessString, err := accessToken.SignedString(accessSecret)
    if err != nil {
        return "", "", err
    }

    refreshString, err := refreshToken.SignedString(refreshSecret)
    if err != nil {
        return "", "", err
    }

    return accessString, refreshString, nil
}

func ParseAccessToken(tokenStr string) (*AccessTokenClaims, error) {
    token, err := jwt.ParseWithClaims(tokenStr, &AccessTokenClaims{}, func(t *jwt.Token) (interface{}, error) {
        return accessSecret, nil
    })
    if err != nil {
        return nil, err
    }

    claims, ok := token.Claims.(*AccessTokenClaims)
    if !ok || !token.Valid {
        return nil, jwt.ErrTokenInvalidClaims
    }

    return claims, nil
}

func ParseRefreshToken(tokenStr string) (*RefreshTokenClaims, error) {
    token, err := jwt.ParseWithClaims(tokenStr, &RefreshTokenClaims{}, func(t *jwt.Token) (interface{}, error) {
        return refreshSecret, nil
    })
    if err != nil {
        return nil, err
    }

    claims, ok := token.Claims.(*RefreshTokenClaims)
    if !ok || !token.Valid {
        return nil, jwt.ErrTokenInvalidClaims
    }

    return claims, nil
}
