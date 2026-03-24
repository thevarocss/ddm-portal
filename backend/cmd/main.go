package main

import (
    "context"
    "log"
    "os"

    "ddm-portal-backend/controllers"
    "ddm-portal-backend/middleware"

    "github.com/gin-contrib/cors"
    "github.com/gin-gonic/gin"
    "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
    connStr := os.Getenv("DATABASE_URL")
    if connStr == "" {
        log.Fatal("DATABASE_URL is not set")
    }

    pool, err := pgxpool.New(context.Background(), connStr)
    if err != nil {
        log.Fatal("Failed to connect to database:", err)
    }
    defer pool.Close()

    r := gin.Default()

    r.Use(cors.New(cors.Config{
        AllowOrigins:     []string{"http://62.164.214.102:3000"},
        AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
        AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
        ExposeHeaders:    []string{"Content-Length"},
        AllowCredentials: true,
    }))

    // -------------------------
    // AUTH ROUTES (Primary)
    // -------------------------
    r.POST("/auth/register", controllers.RegisterHandler(pool))
    r.POST("/auth/login", controllers.LoginHandler(pool))
    r.POST("/auth/refresh", controllers.RefreshTokenHandler(pool))
    r.GET("/auth/me", middleware.RequireAuth(), controllers.MeHandler())

    // -------------------------
    // AUTH ROUTES (API PREFIX)
    // -------------------------
    apiAuth := r.Group("/api/auth")
    {
        apiAuth.POST("/register", controllers.RegisterHandler(pool))
        apiAuth.POST("/login", controllers.LoginHandler(pool))
        apiAuth.POST("/refresh", controllers.RefreshTokenHandler(pool))
        apiAuth.GET("/me", middleware.RequireAuth(), controllers.MeHandler())
        apiAuth.POST("/reset/request", controllers.RequestPasswordResetHandler(pool))
        apiAuth.POST("/reset/confirm", controllers.ConfirmPasswordResetHandler(pool))
    }

    // -------------------------
    // PASSWORD RESET
    // -------------------------
    r.POST("/auth/reset/request", controllers.RequestPasswordResetHandler(pool))
    r.POST("/auth/reset/confirm", controllers.ConfirmPasswordResetHandler(pool))

    // -------------------------
    // PROTECTED API ROUTES
    // -------------------------
    api := r.Group("/api")
    api.Use(middleware.RequireAuth())

    api.GET("/profile", controllers.MeHandler())

// -------------------------
// DASHBOARD ROUTES (protected)
// -------------------------
dashboard := api.Group("/dashboard")
{
    dashboard.GET("/revenue", controllers.DailyRevenueHandler(pool))
    dashboard.GET("/active", controllers.ActiveSubscribersHandler(pool))
    dashboard.GET("/trends", controllers.DashboardHandler(pool))
    dashboard.GET("/reports/export", controllers.ExportReportsHandler(pool))
}

// -------------------------
// REPORT ROUTES (protected)
// -------------------------
reports := api.Group("/reports")
{
    // Full reports
    reports.GET("/revenue", controllers.GetServiceRevenueReport(pool))
    reports.GET("/subscription", controllers.GetServiceSubscriptionReport(pool))
    reports.GET("/operation-log", controllers.GetOperationLogReport(pool))
    reports.GET("/subscription/options", controllers.GetSubscriptionOptions(pool))
    reports.GET("/revenue/options", controllers.GetRevenueOptions(pool))

    // CSV exports
    reports.GET("/revenue/export", controllers.ExportServiceRevenueCSV(pool))
    reports.GET("/subscription/export", controllers.ExportServiceSubscriptionCSV(pool))
    reports.GET("/operation-log/export", controllers.ExportOperationLogCSV(pool))

    // Trends
    reports.GET("/subscriptions", controllers.SubscriptionTrendsHandler(pool))
    reports.GET("/unsubscriptions", controllers.UnsubscriptionTrendsHandler(pool))
    reports.GET("/churn", controllers.ChurnHandler(pool))
}

    // -------------------------
    // ADMIN ROUTES
    // -------------------------
    admin := api.Group("/admin")
    admin.Use(middleware.RequireRole("admin"))
    {
        admin.GET("/users", controllers.ListUsersHandler(pool))
        admin.PUT("/users/:id/role", controllers.UpdateUserRoleHandler(pool))
    }

    // -------------------------
    // TEMPORARY PUBLIC ROUTE
    // -------------------------
//    r.GET("/api/dashboard/trends", controllers.DashboardHandler(pool))

    if err := r.Run(":8080"); err != nil {
        log.Fatal("Failed to start server:", err)
    }
}
