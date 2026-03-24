// controllers/user_admin_controller.go
package controllers

import (
    "context"
    "net/http"

    "github.com/gin-gonic/gin"
    "github.com/jackc/pgx/v5/pgxpool"
)

type UserListItem struct {
    ID       int64  `json:"id"`
    Username string `json:"username"`
    Email    string `json:"email"`
    Role     string `json:"role"`
}

type UpdateUserRoleRequest struct {
    Role string `json:"role"`
}

func ListUsersHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        rows, err := pool.Query(context.Background(),
            `SELECT id, username, email, role FROM users ORDER BY id`)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch users"})
            return
        }
        defer rows.Close()

        var users []UserListItem
        for rows.Next() {
            var u UserListItem
            if err := rows.Scan(&u.ID, &u.Username, &u.Email, &u.Role); err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to scan user"})
                return
            }
            users = append(users, u)
        }

        c.JSON(http.StatusOK, users)
    }
}

func UpdateUserRoleHandler(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        userID := c.Param("id")
        var req UpdateUserRoleRequest

        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
            return
        }

        _, err := pool.Exec(context.Background(),
            `UPDATE users SET role=$1 WHERE id=$2`,
            req.Role, userID,
        )
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update role"})
            return
        }

        c.JSON(http.StatusOK, gin.H{"message": "role updated"})
    }
}
