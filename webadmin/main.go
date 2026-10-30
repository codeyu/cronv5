package webadmin

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/codeyu/cronv5"
	"github.com/codeyu/cronv5/internal/utils"
)

//go:embed templates/*
var fs embed.FS

//go:embed css/style.css
var style string

func GetMux(c *cron.Cron) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET  /{$}", indexHandler(c))
	mux.Handle("POST /{$}", indexHandler(c))
	mux.Handle("POST /cleanup-now/{$}", cleanupNowHandler(c))
	mux.Handle("GET  /completed/{$}", completedHandler(c))
	mux.Handle("GET  /entries/{entryID}/{$}", entryHandler(c))
	mux.Handle("POST /entries/{entryID}/{$}", entryHandler(c))
	mux.Handle("GET  /entries/{entryID}/runs/{runID}/{$}", runHandler(c))
	mux.Handle("POST /entries/{entryID}/runs/{runID}/{$}", runHandler(c))
	mux.Handle("GET  /hooks/{hookID}/{$}", hookHandler(c))
	mux.Handle("POST /hooks/{hookID}/{$}", hookHandler(c))
	return mux
}

var funcsMap = template.FuncMap{
	"FmtDate":  func(t time.Time) string { return t.Format(time.DateTime) },
	"ShortDur": func(t time.Time) string { return utils.ShortDur(t) },
	"Nl2br":    nl2br,
}

// nl2br escapes the string and replaces new lines with <br />
func nl2br(v string) template.HTML {
	return template.HTML(strings.ReplaceAll(template.HTMLEscapeString(v), "\n", "<br />"))
}

func redirectTo(w http.ResponseWriter, l string) error {
	w.Header().Set("Location", l)
	w.WriteHeader(http.StatusSeeOther)
	return nil
}

// templates are parsed once, each page is combined with the base layout
var templates = func() map[string]*template.Template {
	pages := []string{"index", "completed", "entry", "run", "hook"}
	out := make(map[string]*template.Template, len(pages))
	for _, page := range pages {
		name := "templates/" + page + ".gohtml"
		out[name] = template.Must(template.New("").Funcs(funcsMap).ParseFS(fs, "templates/base.gohtml", name))
	}
	return out
}()

func render(code int, name string, data any, w http.ResponseWriter) error {
	tmpl, ok := templates[name]
	if !ok {
		return fmt.Errorf("unknown template %s", name)
	}
	var b bytes.Buffer
	if err := tmpl.ExecuteTemplate(&b, "base", data); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(code)
	_, err := w.Write(b.Bytes())
	return err
}

type M func(w http.ResponseWriter, r *http.Request) error

func (m M) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && isCrossOrigin(r) {
		http.Error(w, "cross-origin request rejected", http.StatusForbidden)
		return
	}
	if err := (m)(w, r); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// isCrossOrigin reports whether the request was sent by another site (CSRF protection).
// Requests without Sec-Fetch-Site nor Origin headers (non-browser clients) are allowed.
func isCrossOrigin(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" {
		return site != "same-origin" && site != "none"
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		return err != nil || u.Host != r.Host
	}
	return false
}

type commonData struct {
	Css       template.CSS
	Now       time.Time
	CleanupTS time.Time
}

func (d *commonData) setCommonData(c *cron.Cron) {
	d.CleanupTS = c.GetCleanupTS()
	d.Now = time.Now()
	d.Css = template.CSS(style)
}

type indexData struct {
	commonData
	JobRuns []cron.JobRun
	Entries []cron.Entry
	Hooks   []cron.Hook
}

type completedData struct {
	commonData
	CompletedJobRuns []cron.JobRun
	Limit            int
}

type entryData struct {
	commonData
	Entry            cron.Entry
	JobRuns          []cron.JobRun
	CompletedJobRuns []cron.JobRun
}

type runData struct {
	commonData
	JobRun cron.JobRun
	Entry  cron.Entry
}

type hookData struct {
	commonData
	Hook cron.Hook
}

func indexHandler(c *cron.Cron) http.Handler {
	return M(func(w http.ResponseWriter, r *http.Request) error {
		if r.Method == http.MethodPost {
			formName := r.PostFormValue("formName")
			if formName == "cancelRun" {
				entryID := cron.EntryID(r.PostFormValue("entryID"))
				runID := cron.RunID(r.PostFormValue("runID"))
				_ = c.CancelJobRun(entryID, runID)
			} else if formName == "removeEntry" {
				entryID := cron.EntryID(r.PostFormValue("entryID"))
				c.Remove(entryID)
			} else if formName == "enableEntry" {
				entryID := cron.EntryID(r.PostFormValue("entryID"))
				c.Enable(entryID)
			} else if formName == "disableEntry" {
				entryID := cron.EntryID(r.PostFormValue("entryID"))
				c.Disable(entryID)
			} else if formName == "runNow" {
				entryID := cron.EntryID(r.PostFormValue("entryID"))
				_ = c.RunNow(entryID)
			} else if formName == "removeHook" {
				hookID := cron.HookID(r.PostFormValue("hookID"))
				c.RemoveHook(hookID)
			} else if formName == "enableHook" {
				hookID := cron.HookID(r.PostFormValue("hookID"))
				c.EnableHook(hookID)
			} else if formName == "disableHook" {
				hookID := cron.HookID(r.PostFormValue("hookID"))
				c.DisableHook(hookID)
			}
			return redirectTo(w, "/")
		}
		jobRuns := c.RunningJobs()
		entries := c.Entries()
		hooks := c.GetHooks()
		data := indexData{
			JobRuns: jobRuns,
			Entries: entries,
			Hooks:   hooks,
		}
		data.setCommonData(c)
		return render(http.StatusOK, "templates/index.gohtml", data, w)
	})
}

func cleanupNowHandler(c *cron.Cron) http.Handler {
	return M(func(w http.ResponseWriter, r *http.Request) error {
		c.CleanupNow()
		return redirectTo(w, r.Referer())
	})
}

func completedHandler(c *cron.Cron) http.Handler {
	return M(func(w http.ResponseWriter, r *http.Request) error {
		limit := 100
		if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil {
			limit = v // limit <= 0 shows all the completed runs
		}
		data := completedData{
			CompletedJobRuns: c.RecentCompletedJobs(limit),
			Limit:            limit,
		}
		data.setCommonData(c)
		return render(http.StatusOK, "templates/completed.gohtml", data, w)
	})
}

func entryHandler(c *cron.Cron) http.Handler {
	return M(func(w http.ResponseWriter, r *http.Request) error {
		entryID := cron.EntryID(r.PathValue("entryID"))
		entry, err := c.Entry(entryID)
		if err != nil {
			return redirectTo(w, "/")
		}
		if r.Method == http.MethodPost {
			formName := r.PostFormValue("formName")
			if formName == "updateLabel" {
				label := r.PostFormValue("label")
				c.UpdateLabel(entryID, label)
			} else if formName == "updateSpec" {
				spec := r.PostFormValue("spec")
				_ = c.UpdateScheduleWithSpec(entryID, spec)
			} else if formName == "enableEntry" {
				c.Enable(entryID)
			} else if formName == "disableEntry" {
				c.Disable(entryID)
			} else if formName == "cancelRun" {
				runID := cron.RunID(r.PostFormValue("runID"))
				_ = c.CancelJobRun(entryID, runID)
			}
			return redirectTo(w, "/entries/"+string(entryID))
		}

		jobRuns, _ := c.RunningJobsFor(entryID)
		completedJobRuns, _ := c.CompletedJobRunsFor(entryID)
		slices.Reverse(completedJobRuns)
		data := entryData{
			Entry:            entry,
			JobRuns:          jobRuns,
			CompletedJobRuns: completedJobRuns,
		}
		data.setCommonData(c)
		return render(http.StatusOK, "templates/entry.gohtml", data, w)
	})
}

func runHandler(c *cron.Cron) http.Handler {
	return M(func(w http.ResponseWriter, r *http.Request) error {
		entryID := cron.EntryID(r.PathValue("entryID"))
		runID := cron.RunID(r.PathValue("runID"))
		entry, entryErr := c.Entry(entryID)
		jobRun, runErr := c.GetJobRun(entryID, runID)
		if entryErr != nil || runErr != nil {
			return redirectTo(w, "/")
		}
		if r.Method == http.MethodPost {
			formName := r.PostFormValue("formName")
			if formName == "cancelRun" {
				_ = c.CancelJobRun(entry.ID, jobRun.RunID)
			}
			return redirectTo(w, "/entries/"+string(entryID)+"/runs/"+string(jobRun.RunID))
		}
		data := runData{
			JobRun: jobRun,
			Entry:  entry,
		}
		data.setCommonData(c)
		return render(http.StatusOK, "templates/run.gohtml", data, w)
	})
}

func hookHandler(c *cron.Cron) http.Handler {
	return M(func(w http.ResponseWriter, r *http.Request) error {
		hookID := cron.HookID(r.PathValue("hookID"))
		hook, err := c.GetHook(hookID)
		if err != nil {
			return redirectTo(w, "/")
		}
		if r.Method == http.MethodPost {
			formName := r.PostFormValue("formName")
			if formName == "disableHook" {
				c.DisableHook(hookID)
			} else if formName == "enableHook" {
				c.EnableHook(hookID)
			} else if formName == "updateLabel" {
				c.SetHookLabel(hookID, r.PostFormValue("label"))
			} else if formName == "removeHook" {
				c.RemoveHook(hookID)
				return redirectTo(w, "/")
			}
			return redirectTo(w, "/hooks/"+string(hookID))
		}
		data := hookData{
			Hook: hook,
		}
		data.setCommonData(c)
		return render(http.StatusOK, "templates/hook.gohtml", data, w)
	})
}
