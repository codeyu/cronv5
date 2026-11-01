package cron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeZoneUTCOffset(t *testing.T) {
	tests := []struct {
		spec   string
		offset int // seconds east of UTC
		name   string
	}{
		{"TZ=UTC+8 0 9 * * *", 8 * 3600, "UTC+08:00"},
		{"CRON_TZ=UTC+08:00 0 9 * * *", 8 * 3600, "UTC+08:00"},
		{"TZ=utc+8 0 9 * * *", 8 * 3600, "UTC+08:00"},
		{"TZ=GMT+8 0 9 * * *", 8 * 3600, "UTC+08:00"},
		{"TZ=UTC-5 0 9 * * *", -5 * 3600, "UTC-05:00"},
		{"TZ=UTC+05:30 0 9 * * *", 5*3600 + 30*60, "UTC+05:30"},
		{"TZ=UTC+5:30 0 9 * * *", 5*3600 + 30*60, "UTC+05:30"},
		{"TZ=UTC+0545 0 9 * * *", 5*3600 + 45*60, "UTC+05:45"},
		{"TZ=+09:00 0 9 * * *", 9 * 3600, "UTC+09:00"},
		{"TZ=-0330 0 9 * * *", -(3*3600 + 30*60), "UTC-03:30"},
		{"TZ=UTC+14 0 9 * * *", 14 * 3600, "UTC+14:00"},
		{"TZ=UTC-12 0 9 * * *", -12 * 3600, "UTC-12:00"},
		{"TZ=UTC+0 0 9 * * *", 0, "UTC+00:00"},
	}
	for _, tt := range tests {
		sched, err := ParseStandard(tt.spec)
		if !assert.NoError(t, err, tt.spec) {
			continue
		}
		loc := sched.(*SpecSchedule).Location
		name, offset := time.Date(2026, 1, 1, 0, 0, 0, 0, loc).Zone()
		assert.Equal(t, tt.offset, offset, tt.spec)
		assert.Equal(t, tt.name, name, tt.spec)
	}

	// "UTC+8" is 8 hours ahead of UTC: 9:00 there is 1:00 UTC
	sched, _ := ParseStandard("TZ=UTC+8 0 9 * * *")
	next := sched.Next(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC), next.UTC())

	// IANA names still work, including the POSIX style "Etc/GMT+8" (8 hours behind UTC)
	sched, err := ParseStandard("TZ=Etc/GMT+8 0 9 * * *")
	assert.NoError(t, err)
	_, offset := time.Date(2026, 1, 1, 0, 0, 0, 0, sched.(*SpecSchedule).Location).Zone()
	assert.Equal(t, -8*3600, offset)
	_, err = ParseStandard("TZ=UTC 0 9 * * *")
	assert.NoError(t, err)
}

func TestTimeZoneUTCOffsetErrors(t *testing.T) {
	for _, spec := range []string{
		"TZ=UTC+15 * * * * *",
		"TZ=UTC+14:30 * * * * *",
		"TZ=UTC+08:60 * * * * *",
		"TZ=UTC+123 * * * * *",
		"TZ=UTC+ * * * * *",
		"TZ=UTC8 * * * * *",
		"TZ=UTC+8:3 * * * * *",
		"TZ=UTC+08:300 * * * * *",
		"TZ=UTC++8 * * * * *",
	} {
		_, err := ParseStandard(spec)
		assert.Error(t, err, spec)
	}
}
