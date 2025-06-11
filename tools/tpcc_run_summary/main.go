package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"pgdistbench/api/benchdriverapi"
	"pgdistbench/pkg/stats"
	"log"
	"os"
	"strings"
)

type Document struct {
	Runners []benchdriverapi.TPCCRunStats `json:"Runners"`
	TPM     stats.DistMetrics             `json:"TPM"`
}

type RunnerSummary struct {
	TpmC            float64 `json:"tpmC"`
	TpmTotal        float64 `json:"tpmTotal"`
	OpsCount        int64   `json:"opsCount"`
	OpsSuccessCount int64   `json:"opsSuccessCount"`
	OpsErrCount     int64   `json:"opsErrCount"`
	OpsTimeMS       float64 `json:"opsTimeMS"`
}

func main() {
	var filenames []string
	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("At least one filename is required as a positional argument.")
	}

	filenames = flag.Args()
	var allSummaries []RunnerSummary
	for _, filename := range filenames {
		summary, err := fileSummary(filename)
		if err != nil {
			log.Fatalf("Failed to get file summary for %s: %v", filename, err)
		}

		allSummaries = append(allSummaries, summary)
	}

	finalStats := combinedStats(allSummaries)

	fmt.Println("\nSummary Table:")
	fmt.Printf("%-15s %-15s %-15s %-20s %-15s %-15s\n", "tpmC", "tpmTotal", "opsCount", "opsSuccessCount", "opsErrCount", "opsTimeMS")
	fmt.Printf("%-15s %-15s %-15s %-20s %-15s %-15s\n", "---------------", "---------------", "---------------", "--------------------", "---------------", "---------------")

	for _, summary := range allSummaries {
		fmt.Printf("%-15.2f %-15.2f %-15d %-20d %-15d %-15.2f\n",
			summary.TpmC,
			summary.TpmTotal,
			summary.OpsCount,
			summary.OpsSuccessCount,
			summary.OpsErrCount,
			summary.OpsTimeMS)
	}

	fmt.Printf("%-15s %-15s %-15s %-20s %-15s %-15s\n", "---------------", "---------------", "---------------", "--------------------", "---------------", "---------------")
	fmt.Printf("%-15s\n%-15.2f %-15.2f %-15.2f %-20.2f %-15.2f %-15.2f\n",
		"Averages",
		finalStats.Averages.TpmC,
		finalStats.Averages.TpmTotal,
		finalStats.Averages.OpsCount,
		finalStats.Averages.OpsSuccessCount,
		finalStats.Averages.OpsErrCount,
		finalStats.Averages.OpsTimeMS)

	fmt.Printf("%-15s\n%-15.2f %-15.2f %-15.2f %-20.2f %-15.2f %-15.2f\n",
		"Medians",
		finalStats.Medians.TpmC,
		finalStats.Medians.TpmTotal,
		finalStats.Medians.OpsCount,
		finalStats.Medians.OpsSuccessCount,
		finalStats.Medians.OpsErrCount,
		finalStats.Medians.OpsTimeMS)
}

func fileSummary(filename string) (RunnerSummary, error) {
	// read from file
	data, err := os.ReadFile(filename)
	if err != nil {
		return RunnerSummary{}, fmt.Errorf("read file: %w", err)
	}

	// Run the summary tool

	var doc Document
	err = json.Unmarshal(data, &doc)
	if err != nil {
		return RunnerSummary{}, fmt.Errorf("unmarshal JSON: %w", err)
	}

	var tpmC, tpmTotal float64
	var opsCount, opsSuccessCount, opsErrCount int64
	var opsTimeMS float64
	for _, runner := range doc.Runners {
		tpmC += float64(runner.Summary.TPMC)
		tpmTotal += float64(runner.Summary.TPMTotal)
		for _, stat := range runner.Stats {
			opsTimeMS += stat.Sum
			if !strings.HasSuffix(stat.Operation, "_ERR") {
				opsSuccessCount += stat.Count
			} else {
				opsErrCount += stat.Count
			}
			opsCount += stat.Count
		}
	}

	return RunnerSummary{
		TpmC:            tpmC,
		TpmTotal:        tpmTotal,
		OpsCount:        opsCount,
		OpsSuccessCount: opsSuccessCount,
		OpsErrCount:     opsErrCount,
		OpsTimeMS:       opsTimeMS,
	}, nil
}

type FinalStats struct {
	Averages CombinedStats `json:"averages"`
	Medians  CombinedStats `json:"medians"`
}

type CombinedStats struct {
	TpmC            float64 `json:"tpmC"`
	TpmTotal        float64 `json:"tpmTotal"`
	OpsCount        float64 `json:"opsCount"`
	OpsSuccessCount float64 `json:"opsSuccessCount"`
	OpsErrCount     float64 `json:"opsErrCount"`
	OpsTimeMS       float64 `json:"opsTimeMS"`
}

func combinedStats(allSummaries []RunnerSummary) FinalStats {
	var totalTpmC, totalTpmTotal, totalOpsTimeMS float64
	var totalOpsCount, totalOpsSuccessCount, totalOpsErrCount int64

	for _, summary := range allSummaries {
		totalTpmC += summary.TpmC
		totalTpmTotal += summary.TpmTotal
		totalOpsCount += summary.OpsCount
		totalOpsSuccessCount += summary.OpsSuccessCount
		totalOpsErrCount += summary.OpsErrCount
		totalOpsTimeMS += summary.OpsTimeMS
	}

	count := float64(len(allSummaries))
	avgTpmC := totalTpmC / count
	avgTpmTotal := totalTpmTotal / count
	avgOpsCount := float64(totalOpsCount) / count
	avgOpsSuccessCount := float64(totalOpsSuccessCount) / count
	avgOpsErrCount := float64(totalOpsErrCount) / count
	avgOpsTimeMS := totalOpsTimeMS / count

	// Calculate medians
	medTpmC := stats.SlicesMedianOf(allSummaries, func(s RunnerSummary) float64 { return s.TpmC })
	medTpmTotal := stats.SlicesMedianOf(allSummaries, func(s RunnerSummary) float64 { return s.TpmTotal })
	medOpsCount := stats.SlicesMedianOf(allSummaries, func(s RunnerSummary) float64 { return float64(s.OpsCount) })
	medOpsSuccessCount := stats.SlicesMedianOf(allSummaries, func(s RunnerSummary) float64 { return float64(s.OpsSuccessCount) })
	medOpsErrCount := stats.SlicesMedianOf(allSummaries, func(s RunnerSummary) float64 { return float64(s.OpsErrCount) })
	medOpsTimeMS := stats.SlicesMedianOf(allSummaries, func(s RunnerSummary) float64 { return s.OpsTimeMS })

	// Calculate sums

	return FinalStats{
		Averages: CombinedStats{
			TpmC:            avgTpmC,
			TpmTotal:        avgTpmTotal,
			OpsCount:        avgOpsCount,
			OpsSuccessCount: avgOpsSuccessCount,
			OpsErrCount:     avgOpsErrCount,
			OpsTimeMS:       avgOpsTimeMS,
		},
		Medians: CombinedStats{
			TpmC:            medTpmC,
			TpmTotal:        medTpmTotal,
			OpsCount:        medOpsCount,
			OpsSuccessCount: medOpsSuccessCount,
			OpsErrCount:     medOpsErrCount,
			OpsTimeMS:       medOpsTimeMS,
		},
	}
}
