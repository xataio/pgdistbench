package script

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

type parseState int

const (
	parseStateMetrics parseState = iota
	parseStateSQLStats
	parseStateGeneralStats
	parseStateLatencyStats
)

type parseResult any

type intermediateReportResult struct {
	Data map[string]any
}

type ParsedSQLStat struct {
	Name    string
	Value   float64
	PerSec  float64
	HasRate bool
}

type ParsedGeneralStat struct {
	Name  string
	Value float64
}

type ParsedLatencyStat struct {
	Name  string
	Value float64
}

type sysbenchParser struct {
	// Compiled regex patterns for performance
	intermediateReportRegex *regexp.Regexp
	sqlStatsRegex           *regexp.Regexp
	generalStatsRegex       *regexp.Regexp
	latencyStatsRegex       *regexp.Regexp

	// Current parsing state
	state parseState
}

func newSysbenchParser() *sysbenchParser {
	return &sysbenchParser{
		// Compile regex patterns for intermediate reports
		// Example: "[ 1s ] thds: 2 tps: 1441.25 qps: 23075.05 (r/w/o: 20190.55/0.00/2884.51) lat (ms,99%): 2.00 err/s: 0.00 reconn/s: 0.00"
		intermediateReportRegex: regexp.MustCompile(`^\[\s*(\d+)s\s*\]\s+thds:\s+(\d+)\s+tps:\s+([\d.]+)\s+qps:\s+([\d.]+)\s+\(r/w/o:\s+([\d.]+)/([\d.]+)/([\d.]+)\)\s+lat\s+\(ms,99%\):\s+([\d.]+)\s+err/s:\s+([\d.]+)\s+reconn/s:\s+([\d.]+)`),

		// Regex patterns for final statistics parsing
		sqlStatsRegex:     regexp.MustCompile(`^\s*(read|write|other|total|transactions|queries|ignored errors|reconnects):\s+(\d+)(?:\s+\(([\d.]+)\s+per\s+sec\.\))?`),
		generalStatsRegex: regexp.MustCompile(`^\s*(total time|total number of events):\s+([\d.]+)s?`),
		latencyStatsRegex: regexp.MustCompile(`^\s*(min|avg|max|99th percentile|sum):\s+([\d.]+)`),

		state: parseStateMetrics,
	}
}

func (p *sysbenchParser) ParseLine(line string) parseResult {
	// Update parser state based on section headers
	p.updateParseState(line)

	// Try parsing intermediate reports first
	if p.isIntermediateReportLine(line) {
		if record, err := p.parseIntermediateReport(line); err == nil {
			return &intermediateReportResult{Data: record}
		}
	}

	// Parse final statistics sections based on current state
	switch p.state {
	case parseStateSQLStats:
		if sqlStat, err := p.parseSQLStatsLine(line); err == nil {
			return sqlStat
		}
	case parseStateGeneralStats:
		if generalStat, err := p.parseGeneralStatsLine(line); err == nil {
			return generalStat
		}
	case parseStateLatencyStats:
		if latencyStat, err := p.parseLatencyStatsLine(line); err == nil {
			return latencyStat
		}
	}

	return nil // No match
}

func (p *sysbenchParser) updateParseState(line string) {
	if strings.Contains(line, "SQL statistics:") {
		p.state = parseStateSQLStats
	} else if strings.Contains(line, "General statistics:") {
		p.state = parseStateGeneralStats
	} else if strings.Contains(line, "Latency (ms):") {
		p.state = parseStateLatencyStats
	} else if strings.Contains(line, "Threads fairness:") {
		// End of interesting statistics
		p.state = parseStateMetrics
	}
}

func (p *sysbenchParser) isIntermediateReportLine(line string) bool {
	return p.intermediateReportRegex.MatchString(line)
}

func (p *sysbenchParser) parseIntermediateReport(line string) (map[string]any, error) {
	matches := p.intermediateReportRegex.FindStringSubmatch(line)
	if matches == nil {
		return nil, errors.New("line does not match intermediate report pattern")
	}

	if len(matches) != 11 { // Full match + 10 groups
		return nil, errors.New("invalid number of regex groups")
	}

	// Helper function to parse float with error handling
	parseFloat := func(s string) float64 {
		if val, err := strconv.ParseFloat(s, 64); err == nil {
			return val
		}
		return 0.0
	}

	// Helper function to parse int with error handling
	parseInt := func(s string) int64 {
		if val, err := strconv.ParseInt(s, 10, 64); err == nil {
			return val
		}
		return 0
	}

	// Calculate cumulative counts from rates
	// errors and reconnects should be cumulative counts, not rates per second
	timeSeconds := parseInt(matches[1])
	errorsPerSec := parseFloat(matches[9])
	reconnectsPerSec := parseFloat(matches[10])

	record := map[string]any{
		"time":       timeSeconds,                                    // time in seconds (int64)
		"threads":    parseInt(matches[2]),                           // thread count (int64)
		"tps":        parseFloat(matches[3]),                         // transactions per second (float64)
		"qps":        parseFloat(matches[4]),                         // queries per second (total) (float64)
		"qps_read":   parseFloat(matches[5]),                         // read queries per second (float64)
		"qps_write":  parseFloat(matches[6]),                         // write queries per second (float64)
		"qps_other":  parseFloat(matches[7]),                         // other queries per second (float64)
		"lat_99th":   parseFloat(matches[8]),                         // 99th percentile latency in ms (float64)
		"errors":     int64(errorsPerSec * float64(timeSeconds)),     // cumulative error count (int64)
		"reconnects": int64(reconnectsPerSec * float64(timeSeconds)), // cumulative reconnect count (int64)
	}

	return record, nil
}

func (p *sysbenchParser) parseSQLStatsLine(line string) (*ParsedSQLStat, error) {
	matches := p.sqlStatsRegex.FindStringSubmatch(line)
	if matches == nil {
		return nil, errors.New("line does not match SQL stats pattern")
	}

	if len(matches) < 3 {
		return nil, errors.New("insufficient regex groups for SQL stats")
	}

	statName := strings.ReplaceAll(matches[1], " ", "_")
	value, err := strconv.ParseFloat(matches[2], 64)
	if err != nil {
		return nil, err
	}

	result := &ParsedSQLStat{
		Name:    statName,
		Value:   value,
		HasRate: false,
	}

	// Also parse per-second rate if available
	if len(matches) > 3 && matches[3] != "" {
		if rate, err := strconv.ParseFloat(matches[3], 64); err == nil {
			result.PerSec = rate
			result.HasRate = true
		}
	}

	return result, nil
}

func (p *sysbenchParser) parseGeneralStatsLine(line string) (*ParsedGeneralStat, error) {
	matches := p.generalStatsRegex.FindStringSubmatch(line)
	if matches == nil {
		return nil, errors.New("line does not match general stats pattern")
	}

	if len(matches) < 3 {
		return nil, errors.New("insufficient regex groups for general stats")
	}

	statName := strings.ReplaceAll(matches[1], " ", "_")
	value, err := strconv.ParseFloat(matches[2], 64)
	if err != nil {
		return nil, err
	}

	return &ParsedGeneralStat{
		Name:  statName,
		Value: value,
	}, nil
}

func (p *sysbenchParser) parseLatencyStatsLine(line string) (*ParsedLatencyStat, error) {
	matches := p.latencyStatsRegex.FindStringSubmatch(line)
	if matches == nil {
		return nil, errors.New("line does not match latency stats pattern")
	}

	if len(matches) < 3 {
		return nil, errors.New("insufficient regex groups for latency stats")
	}

	statName := strings.ReplaceAll(matches[1], " ", "_")
	statName = strings.ReplaceAll(statName, "th_percentile", "th")
	value, err := strconv.ParseFloat(matches[2], 64)
	if err != nil {
		return nil, err
	}

	return &ParsedLatencyStat{
		Name:  statName,
		Value: value,
	}, nil
}
