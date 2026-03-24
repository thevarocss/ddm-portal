// controllers/reports_endpoints.go
package controllers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

//
// REVENUE
//

// GetServiceRevenueReport returns JSON data for the revenue table with filters and a Total row
func GetServiceRevenueReport(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		from := c.Query("from")
		to := c.Query("to")
		productNameFilter := c.Query("productName")
		serviceNameFilter := c.Query("serviceName")

		productCond := ""
		serviceCond := ""
		args := []interface{}{from, to}

		if productNameFilter != "" && productNameFilter != "ALL" {
			productCond = "AND product_name = $3"
			args = append(args, productNameFilter)
		}
		if serviceNameFilter != "" && serviceNameFilter != "ALL" {
			serviceCond = "AND service_name = $4"
			args = append(args, serviceNameFilter)
		}

		query := fmt.Sprintf(`
            SELECT day, network, product_name, service_name,
                   total_charge_count, new_optins_revenue,
                   renewal_revenue, total_revenue
            FROM mv_daily_revenue
            WHERE day BETWEEN $1 AND $2
              %s %s
            ORDER BY day DESC
        `, productCond, serviceCond)

		rows, err := pool.Query(context.Background(), query, args...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			var day time.Time
			var network, productName, serviceName string
			var totalChargeCount int64
			var newOptinsRevenue, renewalRevenue, totalRevenue float64

			if err := rows.Scan(&day, &network, &productName, &serviceName,
				&totalChargeCount, &newOptinsRevenue, &renewalRevenue, &totalRevenue); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}

			results = append(results, gin.H{
				"Time":             day.Format("2006-01-02"),
				"Network":          network,
				"ProductName":      productName,
				"ServiceName":      serviceName,
				"TotalChargeCount": totalChargeCount,
				"NewOptInsRevenue": newOptinsRevenue,
				"RenewalRevenue":   renewalRevenue,
				"TotalRevenue":     totalRevenue,
			})
		}

		// Totals query
		totalQuery := fmt.Sprintf(`
            SELECT COALESCE(SUM(total_charge_count),0),
                   COALESCE(SUM(new_optins_revenue),0),
                   COALESCE(SUM(renewal_revenue),0),
                   COALESCE(SUM(total_revenue),0)
            FROM mv_daily_revenue
            WHERE day BETWEEN $1 AND $2
              %s %s
        `, productCond, serviceCond)

		totalRow := pool.QueryRow(context.Background(), totalQuery, args...)
		var totalChargeCount int64
		var totalNewOptinsRevenue, totalRenewalRevenue, totalRevenue float64
		if err := totalRow.Scan(&totalChargeCount, &totalNewOptinsRevenue, &totalRenewalRevenue, &totalRevenue); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Append Total row LAST
		results = append(results, gin.H{
			"Time":             "Total",
			"Network":          "ALL",
			"ProductName":      ifEmpty(productNameFilter, "ALL"),
			"ServiceName":      ifEmpty(serviceNameFilter, "ALL"),
			"TotalChargeCount": totalChargeCount,
			"NewOptInsRevenue": totalNewOptinsRevenue,
			"RenewalRevenue":   totalRenewalRevenue,
			"TotalRevenue":     totalRevenue,
		})

		c.JSON(http.StatusOK, results)
	}
}

// ExportServiceRevenueCSV returns CSV for download with filters and a Total row
func ExportServiceRevenueCSV(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		from := c.Query("from")
		to := c.Query("to")
		productNameFilter := c.Query("productName")
		serviceNameFilter := c.Query("serviceName")

		productCond := ""
		serviceCond := ""
		args := []interface{}{from, to}

		if productNameFilter != "" && productNameFilter != "ALL" {
			productCond = "AND product_name = $3"
			args = append(args, productNameFilter)
		}
		if serviceNameFilter != "" && serviceNameFilter != "ALL" {
			serviceCond = "AND service_name = $4"
			args = append(args, serviceNameFilter)
		}

		query := fmt.Sprintf(`
            SELECT day, network, product_name, service_name,
                   total_charge_count, new_optins_revenue,
                   renewal_revenue, total_revenue
            FROM mv_daily_revenue
            WHERE day BETWEEN $1 AND $2
              %s %s
            ORDER BY day DESC
        `, productCond, serviceCond)

		rows, err := pool.Query(context.Background(), query, args...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		csv := "Time,Network,ProductName,ServiceName,TotalChargeCount,NewOptInsRevenue,RenewalRevenue,TotalRevenue\n"
		for rows.Next() {
			var day time.Time
			var network, productName, serviceName string
			var totalChargeCount int64
			var newOptinsRevenue, renewalRevenue, totalRevenue float64

			if err := rows.Scan(&day, &network, &productName, &serviceName,
				&totalChargeCount, &newOptinsRevenue, &renewalRevenue, &totalRevenue); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}

			csv += fmt.Sprintf("%s,%s,%s,%s,%d,%.2f,%.2f,%.2f\n",
				day.Format("2006-01-02"), network, productName, serviceName,
				totalChargeCount, newOptinsRevenue, renewalRevenue, totalRevenue)
		}

		// Totals query
		totalQuery := fmt.Sprintf(`
            SELECT COALESCE(SUM(total_charge_count),0),
                   COALESCE(SUM(new_optins_revenue),0),
                   COALESCE(SUM(renewal_revenue),0),
                   COALESCE(SUM(total_revenue),0)
            FROM mv_daily_revenue
            WHERE day BETWEEN $1 AND $2
              %s %s
        `, productCond, serviceCond)

		totalRow := pool.QueryRow(context.Background(), totalQuery, args...)
		var totalChargeCount int64
		var totalNewOptinsRevenue, totalRenewalRevenue, totalRevenue float64
		if err := totalRow.Scan(&totalChargeCount, &totalNewOptinsRevenue, &totalRenewalRevenue, &totalRevenue); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Append Total row LAST
		csv += fmt.Sprintf("Total,ALL,%s,%s,%d,%.2f,%.2f,%.2f\n",
			ifEmpty(productNameFilter, "ALL"),
			ifEmpty(serviceNameFilter, "ALL"),
			totalChargeCount, totalNewOptinsRevenue, totalRenewalRevenue, totalRevenue)

		c.Header("Content-Type", "text/csv")
		c.Header("Content-Disposition", "attachment; filename=service_revenue.csv")
		c.String(http.StatusOK, csv)
	}
}

//
// SUBSCRIPTION
//

// GetServiceSubscriptionReport returns JSON data for the subscription table with filters and a Total row
func GetServiceSubscriptionReport(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		from := c.Query("from")
		to := c.Query("to")
		productNameFilter := c.Query("productName")
		serviceNameFilter := c.Query("serviceName")

		productCond := ""
		serviceCond := ""
		args := []interface{}{from, to}

		if productNameFilter != "" && productNameFilter != "ALL" {
			productCond = "AND product_name = $3"
			args = append(args, productNameFilter)
		}
		if serviceNameFilter != "" && serviceNameFilter != "ALL" {
			serviceCond = "AND service_name = $4"
			args = append(args, serviceNameFilter)
		}

		// Daily rows already ordered by day DESC
		query := fmt.Sprintf(`
            SELECT day, network, product_name, service_name,
                   free_trial, newly_added_count,
                   total_unsubscribed_count, total_renewal_count
            FROM mv_daily_subscriptions
            WHERE day BETWEEN $1 AND $2
              %s %s
            ORDER BY day DESC
        `, productCond, serviceCond)

		rows, err := pool.Query(context.Background(), query, args...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			var day time.Time
			var network, productName, serviceName string
			var freeTrial, newAdded, unsubscribed, renewal int
			if err := rows.Scan(&day, &network, &productName, &serviceName,
				&freeTrial, &newAdded, &unsubscribed, &renewal); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			results = append(results, gin.H{
				"Time":                   day.Format("2006-01-02"),
				"Network":                network,
				"ProductName":            productName,
				"ServiceName":            serviceName,
				"FreeTrial":              freeTrial,
				"NewlyAddedCount":        newAdded,
				"TotalUnsubscribedCount": unsubscribed,
				"TotalRenewalCount":      renewal,
				"SortOrder":              day.Unix(), // numeric timestamp
			})
		}

		// Totals query
		totalQuery := fmt.Sprintf(`
            SELECT
                COALESCE(SUM(free_trial),0),
                COALESCE(SUM(newly_added_count),0),
                COALESCE(SUM(total_unsubscribed_count),0),
                COALESCE(SUM(total_renewal_count),0)
            FROM mv_daily_subscriptions
            WHERE day BETWEEN $1 AND $2
              %s %s
        `, productCond, serviceCond)

		totalRow := pool.QueryRow(context.Background(), totalQuery, args...)
		var totalFreeTrial, totalNewAdded, totalUnsubscribed, totalRenewal int
		if err := totalRow.Scan(&totalFreeTrial, &totalNewAdded, &totalUnsubscribed, &totalRenewal); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Append Total row LAST
		results = append(results, gin.H{
			"Time":                   "Total",
			"Network":                "ALL",
			"ProductName":            ifEmpty(productNameFilter, "ALL"),
			"ServiceName":            ifEmpty(serviceNameFilter, "ALL"),
			"FreeTrial":              totalFreeTrial,
			"NewlyAddedCount":        totalNewAdded,
			"TotalUnsubscribedCount": totalUnsubscribed,
			"TotalRenewalCount":      totalRenewal,
			"SortOrder":              9999999999, // very large number, always last
		})

		c.JSON(http.StatusOK, results)
	}
}

// helper
func ifEmpty(val, placeholder string) string {
	if val == "" {
		return placeholder
	}
	return val
}

// ExportServiceSubscriptionCSV returns CSV for download with breakdown by product/service and a Total row
func ExportServiceSubscriptionCSV(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		from := c.Query("from")
		to := c.Query("to")
		productNameFilter := c.Query("productName")
		serviceNameFilter := c.Query("serviceName")

		productCond := ""
		serviceCond := ""
		args := []interface{}{from, to}

		if productNameFilter != "" && productNameFilter != "ALL" {
			productCond = "AND product_name = $3"
			args = append(args, productNameFilter)
		}
		if serviceNameFilter != "" && serviceNameFilter != "ALL" {
			serviceCond = "AND service_name = $4"
			args = append(args, serviceNameFilter)
		}

		query := fmt.Sprintf(`
            SELECT day, network, product_name, service_name,
                   free_trial, newly_added_count,
                   total_unsubscribed_count, total_renewal_count
            FROM mv_daily_subscriptions
            WHERE day BETWEEN $1 AND $2
              %s %s
            ORDER BY day DESC
        `, productCond, serviceCond)

		rows, err := pool.Query(context.Background(), query, args...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		csv := "Time,Network,ProductName,ServiceName,FreeTrial,NewlyAddedCount,TotalUnsubscribedCount,TotalRenewalCount\n"
		for rows.Next() {
			var day time.Time
			var network, productName, serviceName string
			var freeTrial, newAdded, unsubscribed, renewal int
			if err := rows.Scan(&day, &network, &productName, &serviceName,
				&freeTrial, &newAdded, &unsubscribed, &renewal); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			csv += fmt.Sprintf("%s,%s,%s,%s,%d,%d,%d,%d\n",
				day.Format("2006-01-02"), network, productName, serviceName,
				freeTrial, newAdded, unsubscribed, renewal)
		}

		// Totals
		totalQuery := fmt.Sprintf(`
            SELECT
                COALESCE(SUM(free_trial),0),
                COALESCE(SUM(newly_added_count),0),
                COALESCE(SUM(total_unsubscribed_count),0),
                COALESCE(SUM(total_renewal_count),0)
            FROM mv_daily_subscriptions
            WHERE day BETWEEN $1 AND $2
              %s %s
        `, productCond, serviceCond)

		totalRow := pool.QueryRow(context.Background(), totalQuery, args...)
		var totalFreeTrial, totalNewAdded, totalUnsubscribed, totalRenewal int
		if err := totalRow.Scan(&totalFreeTrial, &totalNewAdded, &totalUnsubscribed, &totalRenewal); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		csv += fmt.Sprintf("Total,ALL,%s,%s,%d,%d,%d,%d\n",
			ifEmpty(productNameFilter, "ALL"),
			ifEmpty(serviceNameFilter, "ALL"),
			totalFreeTrial, totalNewAdded, totalUnsubscribed, totalRenewal)

		c.Header("Content-Type", "text/csv")
		c.Header("Content-Disposition", "attachment; filename=service_subscription.csv")
		c.String(http.StatusOK, csv)
	}
}

//
// OPERATION LOG
//

func GetOperationLogReport(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		from := c.Query("from")
		to := c.Query("to")
		msisdn := c.Query("msisdn")
		serviceId := c.Query("serviceId")
		sequenceNo := c.Query("sequenceNo")
		requestNo := c.Query("requestNo")

		rows, err := pool.Query(context.Background(), `
            SELECT
                c.received_at,
                COALESCE(c.msisdn, c.payload_msisdn) AS msisdn,
                c.payload_serviceId,
                c.payload_appliedPlan,
                COALESCE(c.payload_sequence, '') AS sequence_no,
                COALESCE(c.payload_request, '') AS request_no,
                c.payload_serviceType,
                s.status,
                c.payload::text AS payload
            FROM ddm_callbacks c
            LEFT JOIN ddm_subscribers s ON s.msisdn = COALESCE(c.msisdn, c.payload_msisdn)
            WHERE c.received_at BETWEEN $1 AND $2
              AND ($3 = '' OR COALESCE(c.msisdn, c.payload_msisdn) = $3)
              AND ($4 = '' OR c.payload_serviceId = $4)
              AND ($5 = '' OR COALESCE(c.payload_sequence, '') = $5)
              AND ($6 = '' OR COALESCE(c.payload_request, '') = $6)
            ORDER BY c.received_at DESC
        `, from, to, msisdn, serviceId, sequenceNo, requestNo)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			var t time.Time
			var msisdnVal, serviceID, productID, seqNo, reqNo, callbackType, status, payload string
			if err := rows.Scan(&t, &msisdnVal, &serviceID, &productID, &seqNo, &reqNo, &callbackType, &status, &payload); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			results = append(results, gin.H{
				"Time":         t.Format("2006-01-02 15:04:05"),
				"MSISDN":       msisdnVal,
				"ServiceId":    serviceID,
				"ProductId":    productID,
				"SequenceNo":   seqNo,
				"Status":       status,
				"CallbackType": callbackType,
				"Payload":      payload,
			})
		}
		c.JSON(http.StatusOK, results)
	}
}

func ExportOperationLogCSV(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		from := c.Query("from")
		to := c.Query("to")
		msisdn := c.Query("msisdn")
		serviceId := c.Query("serviceId")
		sequenceNo := c.Query("sequenceNo")
		requestNo := c.Query("requestNo")

		rows, err := pool.Query(context.Background(), `
            SELECT
                c.received_at,
                COALESCE(c.msisdn, c.payload_msisdn) AS msisdn,
                c.payload_serviceId,
                c.payload_appliedPlan,
                COALESCE(c.payload_sequence, '') AS sequence_no,
                COALESCE(c.payload_request, '') AS request_no,
                c.payload_serviceType,
                s.status,
                c.payload::text AS payload
            FROM ddm_callbacks c
            LEFT JOIN ddm_subscribers s ON s.msisdn = COALESCE(c.msisdn, c.payload_msisdn)
            WHERE c.received_at BETWEEN $1 AND $2
              AND ($3 = '' OR COALESCE(c.msisdn, c.payload_msisdn) = $3)
              AND ($4 = '' OR c.payload_serviceId = $4)
              AND ($5 = '' OR COALESCE(c.payload_sequence, '') = $5)
              AND ($6 = '' OR COALESCE(c.payload_request, '') = $6)
            ORDER BY c.received_at DESC
        `, from, to, msisdn, serviceId, sequenceNo, requestNo)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		csv := "Time,MSISDN,ServiceId,ProductId,SequenceNo,Status,CallbackType,Payload\n"
		for rows.Next() {
			var t time.Time
			var msisdnVal, serviceID, productID, seqNo, reqNo, callbackType, status, payload string
			if err := rows.Scan(&t, &msisdnVal, &serviceID, &productID, &seqNo, &reqNo, &callbackType, &status, &payload); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			// Quote payload to avoid CSV breaking on commas
			csv += fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,\"%s\"\n",
				t.Format("2006-01-02 15:04:05"),
				msisdnVal, serviceID, productID, seqNo, status, callbackType, payload)
		}

		c.Header("Content-Type", "text/csv")
		c.Header("Content-Disposition", "attachment; filename=operation_log.csv")
		c.String(http.StatusOK, csv)
	}
}

// GetSubscriptionOptions returns distinct product and service names for filters
func GetSubscriptionOptions(pool *pgxpool.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Query distinct product_name and service_name from the materialized view
		rows, err := pool.Query(context.Background(), `
            SELECT DISTINCT product_name, service_name
            FROM mv_daily_subscriptions
            ORDER BY product_name, service_name
        `)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		productSet := make(map[string]struct{})
		serviceSet := make(map[string]struct{})

		for rows.Next() {
			var productName, serviceName string
			if err := rows.Scan(&productName, &serviceName); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			if productName != "" {
				productSet[productName] = struct{}{}
			}
			if serviceName != "" {
				serviceSet[serviceName] = struct{}{}
			}
		}

		// Convert sets to slices
		products := make([]string, 0, len(productSet))
		for p := range productSet {
			products = append(products, p)
		}
		services := make([]string, 0, len(serviceSet))
		for s := range serviceSet {
			services = append(services, s)
		}

		c.JSON(http.StatusOK, gin.H{
			"products": products,
			"services": services,
		})
	}
}

// GetRevenueOptions returns distinct product and service names for revenue filters
func GetRevenueOptions(pool *pgxpool.Pool) gin.HandlerFunc {
    return func(c *gin.Context) {
        rows, err := pool.Query(context.Background(), `
            SELECT DISTINCT product_name, service_name
            FROM mv_daily_revenue
            ORDER BY product_name, service_name
        `)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }
        defer rows.Close()

        productSet := make(map[string]struct{})
        serviceSet := make(map[string]struct{})

        for rows.Next() {
            var productName, serviceName string
            if err := rows.Scan(&productName, &serviceName); err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
                return
            }
            if productName != "" {
                productSet[productName] = struct{}{}
            }
            if serviceName != "" {
                serviceSet[serviceName] = struct{}{}
            }
        }

        products := make([]string, 0, len(productSet))
        for p := range productSet {
            products = append(products, p)
        }
        services := make([]string, 0, len(serviceSet))
        for s := range serviceSet {
            services = append(services, s)
        }

        c.JSON(http.StatusOK, gin.H{
            "products": products,
            "services": services,
        })
    }
}

