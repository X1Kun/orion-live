package middleware

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

func TestAuth(t *testing.T) {
	const secret = "test-secret-with-at-least-32-characters"
	now := time.Now().UTC()

	tests := []struct {
		name       string
		header     func(*testing.T) string
		wantStatus int
		wantUserID uint64
	}{
		{name: "missing header", wantStatus: http.StatusUnauthorized},
		{name: "malformed bearer", header: func(*testing.T) string { return "Basic credentials" }, wantStatus: http.StatusUnauthorized},
		{
			name: "expired token",
			header: func(t *testing.T) string {
				return "Bearer " + signAuthToken(t, jwt.SigningMethodHS256, secret, "42", now.Add(-time.Hour), now.Add(-time.Minute))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "wrong signature",
			header: func(t *testing.T) string {
				return "Bearer " + signAuthToken(t, jwt.SigningMethodHS256, "different-secret-with-at-least-32-characters", "42", now, now.Add(time.Hour))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "wrong signing method",
			header: func(t *testing.T) string {
				return "Bearer " + signAuthToken(t, jwt.SigningMethodHS384, secret, "42", now, now.Add(time.Hour))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing expiration",
			header: func(t *testing.T) string {
				claims := jwt.RegisteredClaims{Subject: "42", IssuedAt: jwt.NewNumericDate(now)}
				return "Bearer " + signClaims(t, jwt.SigningMethodHS256, secret, claims)
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid subject",
			header: func(t *testing.T) string {
				return "Bearer " + signAuthToken(t, jwt.SigningMethodHS256, secret, "not-a-user", now, now.Add(time.Hour))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "valid token",
			header: func(t *testing.T) string {
				return "Bearer " + signAuthToken(t, jwt.SigningMethodHS256, secret, "42", now, now.Add(time.Hour))
			},
			wantStatus: http.StatusOK,
			wantUserID: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			nextCalled := false
			router := gin.New()
			router.GET("/protected", Auth(secret), func(c *gin.Context) {
				nextCalled = true
				userID, _ := c.Get(UserIDKey)
				c.String(http.StatusOK, strconv.FormatUint(userID.(uint64), 10))
			})

			request := httptest.NewRequest(http.MethodGet, "/protected", nil)
			if tt.header != nil {
				request.Header.Set("Authorization", tt.header(t))
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, tt.wantStatus, response.Body.String())
			}
			if nextCalled != (tt.wantStatus == http.StatusOK) {
				t.Fatalf("next handler called = %v", nextCalled)
			}
			if tt.wantUserID != 0 && response.Body.String() != strconv.FormatUint(tt.wantUserID, 10) {
				t.Fatalf("user ID body = %q, want %d", response.Body.String(), tt.wantUserID)
			}
		})
	}
}

func signAuthToken(t *testing.T, method jwt.SigningMethod, secret, subject string, issuedAt, expiresAt time.Time) string {
	t.Helper()
	return signClaims(t, method, secret, jwt.RegisteredClaims{
		Subject:   subject,
		IssuedAt:  jwt.NewNumericDate(issuedAt),
		ExpiresAt: jwt.NewNumericDate(expiresAt),
	})
}

func signClaims(t *testing.T, method jwt.SigningMethod, secret string, claims jwt.RegisteredClaims) string {
	t.Helper()
	token, err := jwt.NewWithClaims(method, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return token
}
