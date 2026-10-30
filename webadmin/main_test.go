package webadmin

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/codeyu/cronv5"
	"github.com/stretchr/testify/assert"
)

func TestNl2brEscapesHTML(t *testing.T) {
	assert.Equal(t, "&lt;script&gt;alert(1)&lt;/script&gt;<br />line2", string(nl2br("<script>alert(1)</script>\nline2")))
}

func TestRunPageEscapesLogs(t *testing.T) {
	c := cron.New().WithKeepCompletedRunsDur(time.Hour).Quiet().Build()
	c.Start()
	defer c.Stop()
	id, _ := c.AddJob("@every 1h", func(jr cron.JobRun) {
		jr.Logger().Info("<script>alert(1)</script>")
	})
	assert.NoError(t, c.RunNow(id))
	assert.Eventually(t, func() bool { return len(c.CompletedJobs()) == 1 }, time.Second, time.Millisecond)
	run := c.CompletedJobs()[0]

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/entries/"+string(id)+"/runs/"+string(run.RunID)+"/", nil)
	GetMux(c).ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.NotContains(t, body, "<script>alert(1)</script>")
	assert.Contains(t, body, "&lt;script&gt;")
}

func TestCrossOriginPostRejected(t *testing.T) {
	c := cron.New().Quiet().Build()
	id, _ := c.AddJob("@every 1h", func() {}, cron.WithID("entry"))
	newReq := func(headers map[string]string) *http.Request {
		form := url.Values{"formName": {"disableEntry"}}
		req := httptest.NewRequest(http.MethodPost, "http://example.com/entries/"+string(id)+"/", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		return req
	}
	tests := []struct {
		name    string
		headers map[string]string
		code    int
	}{
		{"cross-site fetch metadata", map[string]string{"Sec-Fetch-Site": "cross-site"}, http.StatusForbidden},
		{"same-site fetch metadata", map[string]string{"Sec-Fetch-Site": "same-site"}, http.StatusForbidden},
		{"foreign origin", map[string]string{"Origin": "http://evil.com"}, http.StatusForbidden},
		{"same origin fetch metadata", map[string]string{"Sec-Fetch-Site": "same-origin"}, http.StatusSeeOther},
		{"same origin", map[string]string{"Origin": "http://example.com"}, http.StatusSeeOther},
		{"non-browser client", nil, http.StatusSeeOther},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			GetMux(c).ServeHTTP(rec, newReq(tt.headers))
			assert.Equal(t, tt.code, rec.Code)
		})
	}
}
