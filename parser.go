package cron

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Configuration options for creating a parser. Most options specify which
// fields should be included, while others enable features. If a field is not
// included the parser will assume a default value. These options do not change
// the order fields are parse in.
type ParseOption int

const (
	Second         ParseOption = 1 << iota // Seconds field, default 0
	SecondOptional                         // Optional seconds field, default 0
	Minute                                 // Minutes field, default 0
	Hour                                   // Hours field, default 0
	Dom                                    // Day of month field, default *
	Month                                  // Month field, default *
	Dow                                    // Day of week field, default *
	DowOptional                            // Optional day of week field, default *
	Descriptor                             // Allow descriptors such as @monthly, @weekly, etc.
)

var places = []ParseOption{
	Second,
	Minute,
	Hour,
	Dom,
	Month,
	Dow,
}

var defaults = []string{
	"0",
	"0",
	"0",
	"*",
	"*",
	"*",
}

// A custom Parser that can be configured.
type Parser struct {
	options ParseOption
}

// NewParser creates a Parser with custom options.
//
// It panics if more than one Optional is given, since it would be impossible to
// correctly infer which optional is provided or missing in general.
//
// Examples
//
//	// Standard parser without descriptors
//	specParser := NewParser(Minute | Hour | Dom | Month | Dow)
//	sched, err := specParser.Parse("0 0 15 */3 *")
//
//	// Same as above, just excludes time fields
//	specParser := NewParser(Dom | Month | Dow)
//	sched, err := specParser.Parse("15 */3 *")
//
//	// Same as above, just makes Dow optional
//	specParser := NewParser(Dom | Month | DowOptional)
//	sched, err := specParser.Parse("15 */3")
func NewParser(options ParseOption) Parser {
	optionals := 0
	if options&DowOptional > 0 {
		optionals++
	}
	if options&SecondOptional > 0 {
		optionals++
	}
	if optionals > 1 {
		panic("multiple optionals may not be configured")
	}
	return Parser{options}
}

// Parse returns a new crontab schedule representing the given spec.
// It returns a descriptive error if the spec is not valid.
// It accepts crontab specs and features configured by NewParser.
func (p Parser) Parse(spec string) (Schedule, error) {
	if len(spec) == 0 {
		return nil, fmt.Errorf("empty spec string")
	}

	// Extract timezone if present
	var loc = time.Local
	if strings.HasPrefix(spec, "TZ=") || strings.HasPrefix(spec, "CRON_TZ=") {
		var err error
		i := strings.Index(spec, " ")
		if i == -1 {
			return nil, fmt.Errorf("invalid spec string")
		}
		eq := strings.Index(spec, "=")
		if eq+1 == i {
			return nil, fmt.Errorf("empty location: %s", spec)
		}
		if loc, err = loadLocation(spec[eq+1 : i]); err != nil {
			return nil, fmt.Errorf("provided bad location %s: %v", spec[eq+1:i], err)
		}
		spec = strings.TrimSpace(spec[i:])
	}

	// Handle named schedules (descriptors), if configured
	if strings.HasPrefix(spec, "@") {
		if p.options&Descriptor == 0 {
			return nil, fmt.Errorf("parser does not accept descriptors: %v", spec)
		}
		return parseDescriptor(spec, loc)
	}

	// Split on whitespace.
	fields := strings.Fields(spec)

	// Validate & fill in any omitted or optional fields
	var err error
	fields, err = normalizeFields(fields, p.options)
	if err != nil {
		return nil, err
	}

	sched := &SpecSchedule{Location: loc}
	for _, f := range fields[:3] {
		// Like Vixie cron, a time field starting with a star makes an interval schedule ("*/5", "*").
		if strings.HasPrefix(f, "*") || strings.HasPrefix(f, "?") {
			sched.intervalTime = true
		}
	}
	field := func(field string, r bounds, special specialItemParser) uint64 {
		if err != nil {
			return 0
		}
		var bits uint64
		bits, err = getFieldWith(field, r, &sched.days, special)
		return bits
	}

	sched.Second = field(fields[0], seconds, nil)
	sched.Minute = field(fields[1], minutes, nil)
	sched.Hour = field(fields[2], hours, nil)
	sched.Dom = field(fields[3], dom, parseDomItem)
	sched.Month = field(fields[4], months, nil)
	sched.Dow = field(fields[5], dow, parseDowItem)
	if err != nil {
		return nil, err
	}
	if (sched.Second|sched.Minute|sched.Hour)&starBit != 0 {
		sched.intervalTime = false // already known from the bits
	}
	return sched, nil
}

// locationCache caches time.LoadLocation, which reads the tz database on every call.
// Only valid locations are cached, so the cache is bounded by the size of the tz database.
var locationCache sync.Map // map[string]*time.Location

func loadLocation(name string) (*time.Location, error) {
	if loc, ok := locationCache.Load(name); ok {
		return loc.(*time.Location), nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	locationCache.Store(name, loc)
	return loc, nil
}

// normalizeFields takes a subset set of the time fields and returns the full set
// with defaults (zeroes) populated for unset fields.
//
// As part of performing this function, it also validates that the provided
// fields are compatible with the configured options.
func normalizeFields(fields []string, options ParseOption) ([]string, error) {
	// Validate optionals & add their field to options
	optionals := 0
	if options&SecondOptional > 0 {
		options |= Second
		optionals++
	}
	if options&DowOptional > 0 {
		options |= Dow
		optionals++
	}
	if optionals > 1 {
		return nil, fmt.Errorf("multiple optionals may not be configured")
	}

	// Figure out how many fields we need
	max := 0
	for _, place := range places {
		if options&place > 0 {
			max++
		}
	}
	min := max - optionals

	// Validate number of fields
	if count := len(fields); count < min || count > max {
		if min == max {
			return nil, fmt.Errorf("expected exactly %d fields, found %d: %s", min, count, fields)
		}
		return nil, fmt.Errorf("expected %d to %d fields, found %d: %s", min, max, count, fields)
	}

	// Populate the optional field if not provided
	if min < max && len(fields) == min {
		switch {
		case options&DowOptional > 0:
			fields = append(fields, defaults[5]) // TODO: improve access to default
		case options&SecondOptional > 0:
			fields = append([]string{defaults[0]}, fields...)
		}
	}

	// Populate all fields not part of options with their defaults
	n := 0
	expandedFields := make([]string, len(places))
	copy(expandedFields, defaults)
	for i, place := range places {
		if options&place > 0 {
			expandedFields[i] = fields[n]
			n++
		}
	}
	return expandedFields, nil
}

var standardParser = NewParser(
	Minute | Hour | Dom | Month | Dow | Descriptor,
)

var SecondParser = NewParser(Second | Minute | Hour | Dom | Month | DowOptional | Descriptor)

// ParseStandard returns a new crontab schedule representing the given
// standardSpec (https://en.wikipedia.org/wiki/Cron). It requires 5 entries
// representing: minute, hour, day of month, month and day of week, in that
// order. It returns a descriptive error if the spec is not valid.
//
// It accepts
//   - Standard crontab specs, e.g. "* * * * ?"
//   - Descriptors, e.g. "@midnight", "@every 1h30m"
func ParseStandard(standardSpec string) (Schedule, error) {
	return standardParser.Parse(standardSpec)
}

// getField returns an Int with the bits set representing all of the times that
// the field represents or error parsing field value.  A "field" is a comma-separated
// list of "ranges".
func getField(field string, r bounds) (uint64, error) {
	return getFieldWith(field, r, nil, nil)
}

// specialItemParser parses the items of a field that cannot be represented as bits
// (eg: "L", "15W", "MON#2") into days. It returns false if expr is not such an item.
type specialItemParser func(expr string, days *dayRules) (bool, error)

func getFieldWith(field string, r bounds, days *dayRules, special specialItemParser) (uint64, error) {
	var bits uint64
	ranges := strings.FieldsFunc(field, func(r rune) bool { return r == ',' })
	for _, expr := range ranges {
		if special != nil && strings.ContainsAny(expr, "LlWw#") {
			if ok, err := special(expr, days); err != nil {
				return bits, err
			} else if ok {
				continue
			}
		}
		bit, err := getRange(expr, r)
		if err != nil {
			return bits, err
		}
		bits |= bit
	}
	return bits, nil
}

// parseDomItem parses the day-of-month items:
//
//	"L"   last day of the month
//	"L-n" n days before the last day of the month (0 <= n <= 30)
//	"LW"  last weekday (Monday to Friday) of the month
//	"nW"  weekday nearest to the n-th day of the month, without leaving the month (1 <= n <= 31)
func parseDomItem(expr string, days *dayRules) (bool, error) {
	upper := strings.ToUpper(expr)
	switch {
	case upper == "L":
		days.lastDom |= 1
	case upper == "LW":
		days.lastWeekdayDom = true
	case strings.HasPrefix(upper, "L-"):
		n, err := mustParseInt(expr[2:])
		if err != nil {
			return false, err
		}
		if n > 30 {
			return false, fmt.Errorf("offset from the last day of the month (%d) above maximum (30): %s", n, expr)
		}
		days.lastDom |= 1 << n
	case strings.HasSuffix(upper, "W"):
		n, err := mustParseInt(expr[:len(expr)-1])
		if err != nil {
			return false, err
		}
		if n < dom.min || n > dom.max {
			return false, fmt.Errorf("day of month (%d) out of range (%d-%d): %s", n, dom.min, dom.max, expr)
		}
		days.weekdayDom |= 1 << n
	case strings.Contains(upper, "L"):
		return false, fmt.Errorf("invalid use of L in day-of-month field: %s", expr)
	default:
		return false, nil
	}
	return true, nil
}

// parseDowItem parses the day-of-week items:
//
//	"d#k" k-th weekday d of the month (1 <= k <= 5), eg: "MON#1" or "1#1"
//	"dL"  last weekday d of the month, eg: "FRIL" or "5L"
func parseDowItem(expr string, days *dayRules) (bool, error) {
	weekday := func(s string) (uint, error) {
		d, err := parseIntOrName(s, dow.names)
		if err != nil {
			return 0, err
		}
		if d == 7 {
			d = 0
		}
		if d > dow.max {
			return 0, fmt.Errorf("day of week (%d) above maximum (7): %s", d, expr)
		}
		return d, nil
	}
	upper := strings.ToUpper(expr)
	if before, after, ok := strings.Cut(upper, "#"); ok {
		d, err := weekday(before)
		if err != nil {
			return false, err
		}
		k, err := mustParseInt(after)
		if err != nil {
			return false, err
		}
		if k < 1 || k > 5 {
			return false, fmt.Errorf("occurrence of the day of week (%d) out of range (1-5): %s", k, expr)
		}
		days.nthDow[d] |= 1 << k
		return true, nil
	}
	if upper == "L" {
		return false, fmt.Errorf("L alone is ambiguous in day-of-week field, use 6 (or SAT) for Saturday or 5L for the last Friday: %s", expr)
	}
	if strings.HasSuffix(upper, "L") {
		d, err := weekday(upper[:len(upper)-1])
		if err != nil {
			return false, err
		}
		days.lastDow |= 1 << d
		return true, nil
	}
	return false, nil
}

// getRange returns the bits indicated by the given expression:
//
//	number | number "-" number [ "/" number ]
//
// or error parsing range.
func getRange(expr string, r bounds) (uint64, error) {
	var (
		start, end, step uint
		rangeAndStep     = strings.Split(expr, "/")
		lowAndHigh       = strings.Split(rangeAndStep[0], "-")
		singleDigit      = len(lowAndHigh) == 1
		err              error
	)

	var extra uint64
	if lowAndHigh[0] == "*" || lowAndHigh[0] == "?" {
		start = r.min
		end = r.max
		extra = starBit
	} else {
		start, err = parseIntOrName(lowAndHigh[0], r.names)
		if err != nil {
			return 0, err
		}
		switch len(lowAndHigh) {
		case 1:
			end = start
		case 2:
			end, err = parseIntOrName(lowAndHigh[1], r.names)
			if err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("too many hyphens: %s", expr)
		}
	}

	switch len(rangeAndStep) {
	case 1:
		step = 1
	case 2:
		step, err = mustParseInt(rangeAndStep[1])
		if err != nil {
			return 0, err
		}

		// Special handling: "N/step" means "N-max/step".
		if singleDigit {
			end = r.max
		}
		if step > 1 {
			extra = 0
		}
	default:
		return 0, fmt.Errorf("too many slashes: %s", expr)
	}

	maxValue := r.max
	if r.sunday7 {
		// 7 is Sunday: "7-2" is "0-2" (SUN-TUE), while "5-7" keeps 7 as the end of the range (FRI-SUN).
		maxValue = 7
		if start == 7 && end != 7 {
			start = 0
		}
	}
	if start < r.min {
		return 0, fmt.Errorf("beginning of range (%d) below minimum (%d): %s", start, r.min, expr)
	}
	if start > maxValue {
		return 0, fmt.Errorf("beginning of range (%d) above maximum (%d): %s", start, maxValue, expr)
	}
	if end > maxValue {
		return 0, fmt.Errorf("end of range (%d) above maximum (%d): %s", end, maxValue, expr)
	}
	if step == 0 {
		return 0, fmt.Errorf("step of range should be a positive number: %s", expr)
	}

	var bits uint64
	if start > end {
		if !r.cyclic || singleDigit {
			return 0, fmt.Errorf("beginning of range (%d) beyond end of range (%d): %s", start, end, expr)
		}
		bits = getCyclicBits(start, end, step, r)
	} else {
		bits = getBits(start, end, step)
	}
	if r.sunday7 && bits&(1<<7) != 0 {
		bits = bits&^(1<<7) | 1<<0
	}
	return bits | extra, nil
}

// getCyclicBits sets the bits of a range wrapping around the end of the field,
// eg: "FRI-MON" (5-1) is FRI, SAT, SUN, MON, modulo the given step size.
func getCyclicBits(start, end, step uint, r bounds) uint64 {
	var (
		bits   uint64
		size   = r.max - r.min + 1
		length = (end+size-start)%size + 1
	)
	for i := uint(0); i < length; i += step {
		bits |= 1 << (r.min + (start-r.min+i)%size)
	}
	return bits
}

// parseIntOrName returns the (possibly-named) integer contained in expr.
func parseIntOrName(expr string, names map[string]uint) (uint, error) {
	if names != nil {
		if namedInt, ok := names[strings.ToLower(expr)]; ok {
			return namedInt, nil
		}
	}
	return mustParseInt(expr)
}

// mustParseInt parses the given expression as an int or returns an error.
func mustParseInt(expr string) (uint, error) {
	num, err := strconv.Atoi(expr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse int from %s: %s", expr, err)
	}
	if num < 0 {
		return 0, fmt.Errorf("negative number (%d) not allowed: %s", num, expr)
	}

	return uint(num), nil
}

// getBits sets all bits in the range [min, max], modulo the given step size.
func getBits(min, max, step uint) uint64 {
	var bits uint64

	// If step is 1, use shifts.
	if step == 1 {
		return ^(math.MaxUint64 << (max + 1)) & (math.MaxUint64 << min)
	}

	// Else, use a simple loop.
	for i := min; i <= max; i += step {
		bits |= 1 << i
	}
	return bits
}

// all returns all bits within the given bounds.  (plus the star bit)
func all(r bounds) uint64 {
	return getBits(r.min, r.max, 1) | starBit
}

// parseDescriptor returns a predefined schedule for the expression, or error if none matches.
func parseDescriptor(descriptor string, loc *time.Location) (Schedule, error) {
	switch descriptor {
	case "@yearly", "@annually":
		return &SpecSchedule{
			Second:   1 << seconds.min,
			Minute:   1 << minutes.min,
			Hour:     1 << hours.min,
			Dom:      1 << dom.min,
			Month:    1 << months.min,
			Dow:      all(dow),
			Location: loc,
		}, nil

	case "@monthly":
		return &SpecSchedule{
			Second:   1 << seconds.min,
			Minute:   1 << minutes.min,
			Hour:     1 << hours.min,
			Dom:      1 << dom.min,
			Month:    all(months),
			Dow:      all(dow),
			Location: loc,
		}, nil

	case "@weekly":
		return &SpecSchedule{
			Second:   1 << seconds.min,
			Minute:   1 << minutes.min,
			Hour:     1 << hours.min,
			Dom:      all(dom),
			Month:    all(months),
			Dow:      1 << dow.min,
			Location: loc,
		}, nil

	case "@daily", "@midnight":
		return &SpecSchedule{
			Second:   1 << seconds.min,
			Minute:   1 << minutes.min,
			Hour:     1 << hours.min,
			Dom:      all(dom),
			Month:    all(months),
			Dow:      all(dow),
			Location: loc,
		}, nil

	case "@hourly":
		return &SpecSchedule{
			Second:   1 << seconds.min,
			Minute:   1 << minutes.min,
			Hour:     all(hours),
			Dom:      all(dom),
			Month:    all(months),
			Dow:      all(dow),
			Location: loc,
		}, nil

	}

	const every = "@every "
	if strings.HasPrefix(descriptor, every) {
		duration, err := time.ParseDuration(descriptor[len(every):])
		if err != nil {
			return nil, fmt.Errorf("failed to parse duration %s: %s", descriptor, err)
		}
		return Every(duration), nil
	}

	return nil, fmt.Errorf("unrecognized descriptor: %s", descriptor)
}
